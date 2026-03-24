<?php
/**
 * SQLite DB Actualizer
 * Сравнивает существующую БД SQLite с эталонным SQL-файлом и устраняет различия.
 *
 * Использование:
 *   php db_actualizer.php --db=path/to/db.sqlite --sql=path/to/db.sql [--dry-run] [--verbose]
 *
 * Опции:
 *   --db       Путь к файлу SQLite базы данных
 *   --sql      Путь к эталонному SQL файлу
 *   --dry-run  Только проверка без внесения изменений
 *   --verbose  Подробный вывод
 */

declare(strict_types=1);

class DbActualizer
{
    /** @var PDO */
    private $db;

    /** @var string */
    private $sqlFile;

    /** @var bool */
    private $dryRun;

    /** @var bool */
    private $verbose;

    /** @var array */
    private $changes = [];

    /** @var array */
    private $errors = [];

    /** @var array Таблицы, которые были (или будут) пересозданы в текущем прогоне */
    private $recreatedTables = [];

    public function __construct(string $dbPath, string $sqlFile, bool $dryRun = false, bool $verbose = false)
    {
        $this->sqlFile = $sqlFile;
        $this->dryRun  = $dryRun;
        $this->verbose = $verbose;

        $this->db = new PDO('sqlite:' . $dbPath);
        $this->db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->db->exec('PRAGMA foreign_keys = OFF');
        $this->db->exec('PRAGMA journal_mode = WAL');
    }

    // -------------------------------------------------------------------------
    // Публичный API
    // -------------------------------------------------------------------------

    /**
     * Запустить актуализацию. Возвращает true если ошибок не было.
     */
    public function run(): bool
    {
        $sql = file_get_contents($this->sqlFile);
        if ($sql === false) {
            $this->errors[] = "Не удалось прочитать файл: {$this->sqlFile}";
            return false;
        }

        $reference = $this->parseSQL($sql);

        $this->syncTables($reference);
        $this->syncViews($reference);
        $this->syncTriggers($reference);

        $this->printSummary();

        return empty($this->errors);
    }

    public function getChanges(): array { return $this->changes; }
    public function getErrors(): array  { return $this->errors; }

    // -------------------------------------------------------------------------
    // Парсинг эталонного SQL
    // -------------------------------------------------------------------------

    /**
     * Разбирает SQL-файл и возвращает структуру:
     * [
     *   'tables'   => [ tableName => [ 'sql' => '...', 'columns' => [...], 'indexes' => [...] ] ],
     *   'views'    => [ viewName  => 'CREATE VIEW ...' ],
     *   'triggers' => [ trigName  => 'CREATE TRIGGER ...' ],
     *   'inserts'  => [ tableName => [ 'INSERT INTO ...' ] ],
     * ]
     */
    public function parseSQL(string $sql): array
    {
        $result = [
            'tables'   => [],
            'views'    => [],
            'triggers' => [],
            'inserts'  => [],
        ];

        // Убираем комментарии
        $sql = preg_replace('/--[^\n]*/', '', $sql);
        $sql = preg_replace('/\/\*.*?\*\//s', '', $sql);

        // Разбиваем на отдельные statements
        $statements = $this->splitStatements($sql);

        foreach ($statements as $stmt) {
            $stmt = trim($stmt);
            if ($stmt === '') continue;

            $upper = strtoupper(ltrim($stmt));

            if (strpos($upper, 'CREATE TABLE') === 0) {
                $name = $this->extractCreateTableName($stmt);
                if ($name !== null) {
                    $result['tables'][$name] = [
                        'sql'     => $stmt,
                        'columns' => $this->parseColumns($stmt),
                        'indexes' => [],
                    ];
                }
            } elseif (strpos($upper, 'CREATE UNIQUE INDEX') === 0 || strpos($upper, 'CREATE INDEX') === 0) {
                $parsed = $this->parseIndexStatement($stmt);
                if ($parsed && isset($result['tables'][$parsed['table']])) {
                    $result['tables'][$parsed['table']]['indexes'][$parsed['name']] = $parsed;
                }
            } elseif (strpos($upper, 'CREATE VIEW') === 0) {
                $name = $this->extractObjectName($stmt, 'VIEW');
                if ($name) $result['views'][$name] = $stmt;
            } elseif (strpos($upper, 'CREATE TRIGGER') === 0) {
                $name = $this->extractObjectName($stmt, 'TRIGGER');
                if ($name) $result['triggers'][$name] = $stmt;
            } elseif (strpos($upper, 'INSERT INTO') === 0) {
                $name = $this->extractInsertTableName($stmt);
                if ($name) $result['inserts'][$name][] = $stmt;
            }
        }

        return $result;
    }

    // -------------------------------------------------------------------------
    // Синхронизация таблиц
    // -------------------------------------------------------------------------

    private function syncTables(array $reference): void
    {
        $existing = $this->getExistingTables();

        // Добавляем отсутствующие таблицы
        foreach ($reference['tables'] as $name => $def) {
            if (!isset($existing[$name])) {
                $this->applyChange("CREATE TABLE `{$name}`", $def['sql']);
                // Индексы новой таблицы
                foreach ($def['indexes'] as $idx) {
                    $this->applyChange("CREATE INDEX `{$idx['name']}` ON `{$name}`", $idx['sql']);
                }
                // Начальные данные
                if (!empty($reference['inserts'][$name])) {
                    foreach ($reference['inserts'][$name] as $ins) {
                        $this->applyChange("INSERT initial data into `{$name}`", $ins);
                    }
                }
                continue;
            }

            // Таблица существует — синхронизируем столбцы и индексы
            $this->syncColumns($name, $def, $existing[$name]);
            $this->syncIndexes($name, $def['indexes']);
        }

        // Удаляем лишние таблицы
        foreach ($existing as $name => $existDef) {
            if (!isset($reference['tables'][$name])) {
                $this->applyChange("DROP TABLE `{$name}`", "DROP TABLE \"{$name}\"");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Синхронизация столбцов
    // -------------------------------------------------------------------------

    private function syncColumns(string $table, array $refDef, array $existDef): void
    {
        $refCols    = $refDef['columns'];
        $existCols  = $this->getExistingColumns($table);

        $refNames   = array_column($refCols, 'name');
        $existNames = array_column($existCols, 'name');

        // Сначала определяем — нужно ли пересоздание таблицы
        $needsRecreate = false;
        $recreateReasons = [];

        foreach ($existCols as $existCol) {
            if (!in_array($existCol['name'], $refNames, true)) {
                $needsRecreate = true;
                $recreateReasons[] = "лишний столбец `{$existCol['name']}`";
            }
        }

        foreach ($refCols as $refCol) {
            $found = null;
            foreach ($existCols as $ec) {
                if ($ec['name'] === $refCol['name']) { $found = $ec; break; }
            }
            if ($found === null) continue; // новый столбец — обрабатывается ниже или через recreation

            if (!$this->columnsMatch($refCol, $found)) {
                $needsRecreate = true;
                $recreateReasons[] = "столбец `{$refCol['name']}` отличается (тип/умолчание/not null/pk)";
            }
        }

        if ($needsRecreate) {
            // Пересоздание покрывает и добавление новых столбцов — ADD COLUMN не нужен
            $reason = implode('; ', $recreateReasons);
            $this->recreateTable($table, $refDef, $reason);
            return;
        }

        // Пересоздание не нужно — добавляем только отсутствующие столбцы
        foreach ($refCols as $col) {
            if (!in_array($col['name'], $existNames, true)) {
                $colDef = $this->buildColumnDef($col);
                $this->applyChange(
                    "ADD COLUMN `{$col['name']}` to `{$table}`",
                    "ALTER TABLE \"{$table}\" ADD COLUMN {$colDef}"
                );
            }
        }
    }

    /**
     * Пересоздание таблицы (единственный способ изменить тип столбца / удалить столбец в SQLite).
     */
    private function recreateTable(string $table, array $refDef, string $reason): void
    {
        $this->logVerbose("Пересоздание таблицы `{$table}`: {$reason}");

        $tmpName = "__tmp_{$table}_" . uniqid();

        // 1. Создать временную таблицу по эталону
        $createTmp = preg_replace(
            '/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?["`\[]?' . preg_quote($table, '/') . '["`\]]?/i',
            "CREATE TABLE \"{$tmpName}\"",
            $refDef['sql']
        );

        $existCols = $this->getExistingColumns($table);
        $refCols   = $refDef['columns'];
        $refNames  = array_column($refCols, 'name');

        // Столбцы для копирования данных (только те, что есть в обеих таблицах)
        $copyNames = [];
        foreach ($existCols as $ec) {
            if (in_array($ec['name'], $refNames, true)) {
                $copyNames[] = '"' . $ec['name'] . '"';
            }
        }
        $colList = implode(', ', $copyNames);

        $ops = [
            ['msg' => "CREATE TEMP TABLE `{$tmpName}` (по эталону `{$table}`)", 'sql' => $createTmp],
            ['msg' => "Копирование данных из `{$table}` в `{$tmpName}`",
             'sql' => "INSERT INTO \"{$tmpName}\" ({$colList}) SELECT {$colList} FROM \"{$table}\""],
            ['msg' => "DROP TABLE `{$table}`", 'sql' => "DROP TABLE \"{$table}\""],
            ['msg' => "ALTER TABLE `{$tmpName}` RENAME TO `{$table}`",
             'sql' => "ALTER TABLE \"{$tmpName}\" RENAME TO \"{$table}\""],
        ];

        if ($this->dryRun) {
            foreach ($ops as $op) {
                $this->changes[] = "[DRY-RUN] {$op['msg']}";
                $this->logVerbose("  [DRY-RUN] {$op['sql']}");
            }
            $this->recreatedTables[] = $table;
            return;
        }

        $this->db->beginTransaction();
        try {
            foreach ($ops as $op) {
                $this->db->exec($op['sql']);
                $this->changes[] = $op['msg'];
                $this->logVerbose("  " . $op['sql']);
            }
            $this->db->commit();
            $this->recreatedTables[] = $table;
        } catch (Exception $e) {
            $this->db->rollBack();
            $this->errors[] = "Ошибка пересоздания таблицы `{$table}`: " . $e->getMessage();
        }
    }

    // -------------------------------------------------------------------------
    // Синхронизация индексов
    // -------------------------------------------------------------------------

    private function syncIndexes(string $table, array $refIndexes): void
    {
        // Если таблица была пересоздана — все старые индексы уже удалены вместе с ней
        $existingIndexes = in_array($table, $this->recreatedTables, true)
            ? []
            : $this->getExistingIndexes($table);

        // Добавляем отсутствующие
        foreach ($refIndexes as $name => $idx) {
            if (!isset($existingIndexes[$name])) {
                $this->applyChange("CREATE INDEX `{$name}` ON `{$table}`", $idx['sql']);
                continue;
            }
            // Индекс существует — проверяем соответствие
            if (!$this->indexesMatch($idx, $existingIndexes[$name])) {
                $this->applyChange("DROP INDEX `{$name}` (изменился)", "DROP INDEX \"{$name}\"");
                $this->applyChange("CREATE INDEX `{$name}` ON `{$table}` (пересоздание)", $idx['sql']);
            }
        }

        // Удаляем лишние (которых нет в эталоне)
        foreach ($existingIndexes as $name => $idx) {
            if (!isset($refIndexes[$name])) {
                $this->applyChange("DROP INDEX `{$name}` (лишний)", "DROP INDEX \"{$name}\"");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Синхронизация views
    // -------------------------------------------------------------------------

    private function syncViews(array $reference): void
    {
        $existing = $this->getExistingObjects('view');

        foreach ($reference['views'] as $name => $sql) {
            if (!isset($existing[$name])) {
                $this->applyChange("CREATE VIEW `{$name}`", $sql);
            } elseif (!$this->sqlNormEqual($existing[$name], $sql)) {
                $this->applyChange("DROP VIEW `{$name}` (изменился)", "DROP VIEW \"{$name}\"");
                $this->applyChange("CREATE VIEW `{$name}` (пересоздание)", $sql);
            }
        }

        foreach ($existing as $name => $sql) {
            if (!isset($reference['views'][$name])) {
                $this->applyChange("DROP VIEW `{$name}` (лишний)", "DROP VIEW \"{$name}\"");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Синхронизация триггеров
    // -------------------------------------------------------------------------

    private function syncTriggers(array $reference): void
    {
        $existing = $this->getExistingObjects('trigger');

        foreach ($reference['triggers'] as $name => $sql) {
            if (!isset($existing[$name])) {
                $this->applyChange("CREATE TRIGGER `{$name}`", $sql);
            } elseif (!$this->sqlNormEqual($existing[$name], $sql)) {
                $this->applyChange("DROP TRIGGER `{$name}` (изменился)", "DROP TRIGGER \"{$name}\"");
                $this->applyChange("CREATE TRIGGER `{$name}` (пересоздание)", $sql);
            }
        }

        foreach ($existing as $name => $sql) {
            if (!isset($reference['triggers'][$name])) {
                $this->applyChange("DROP TRIGGER `{$name}` (лишний)", "DROP TRIGGER \"{$name}\"");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Применение изменения
    // -------------------------------------------------------------------------

    private function applyChange(string $description, string $sql): void
    {
        if ($this->dryRun) {
            $this->changes[] = "[DRY-RUN] {$description}";
            $this->logVerbose("  [DRY-RUN] {$sql}");
            return;
        }

        try {
            $this->db->exec($sql);
            $this->changes[] = $description;
            $this->logVerbose("  {$sql}");
        } catch (Exception $e) {
            $this->errors[] = "Ошибка при «{$description}»: " . $e->getMessage() . "\n  SQL: {$sql}";
        }
    }

    // -------------------------------------------------------------------------
    // Получение текущего состояния БД
    // -------------------------------------------------------------------------

    /**
     * Возвращает [ tableName => ['sql' => 'CREATE TABLE ...'] ]
     */
    public function getExistingTables(): array
    {
        $rows = $this->db->query(
            "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )->fetchAll(PDO::FETCH_ASSOC);

        $result = [];
        foreach ($rows as $row) {
            $result[$row['name']] = ['sql' => $row['sql']];
        }
        return $result;
    }

    /**
     * Возвращает столбцы таблицы через PRAGMA table_info.
     */
    public function getExistingColumns(string $table): array
    {
        $rows = $this->db->query("PRAGMA table_info(\"{$table}\")")->fetchAll(PDO::FETCH_ASSOC);
        $cols = [];
        foreach ($rows as $r) {
            $cols[] = [
                'name'    => $r['name'],
                'type'    => strtoupper(trim($r['type'])),
                'notnull' => (int)$r['notnull'],
                'default' => $r['dflt_value'],
                'pk'      => (int)$r['pk'],
            ];
        }
        return $cols;
    }

    /**
     * Возвращает индексы таблицы [ indexName => ['name'=>..., 'unique'=>..., 'columns'=>[...], 'sql'=>...] ]
     */
    public function getExistingIndexes(string $table): array
    {
        // PRAGMA index_list даёт флаг unique; sqlite_master даёт sql
        $list = $this->db->query("PRAGMA index_list(\"{$table}\")")->fetchAll(PDO::FETCH_ASSOC);

        $result = [];
        foreach ($list as $idx) {
            // Пропускаем неявные индексы (PRIMARY KEY, UNIQUE constraint) — у них sql=NULL
            $sqlRow = $this->db->query(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name='" . str_replace("'", "''", $idx['name']) . "' AND sql IS NOT NULL"
            )->fetch(PDO::FETCH_ASSOC);
            if (!$sqlRow) continue;

            $cols = $this->db->query("PRAGMA index_info(\"{$idx['name']}\")")->fetchAll(PDO::FETCH_ASSOC);
            $result[$idx['name']] = [
                'name'    => $idx['name'],
                'unique'  => (int)$idx['unique'],
                'columns' => array_column($cols, 'name'),
                'sql'     => $sqlRow['sql'],
            ];
        }
        return $result;
    }

    /**
     * Возвращает views или triggers [ name => sql ]
     */
    private function getExistingObjects(string $type): array
    {
        $rows = $this->db->query(
            "SELECT name, sql FROM sqlite_master WHERE type='{$type}' ORDER BY name"
        )->fetchAll(PDO::FETCH_ASSOC);

        $result = [];
        foreach ($rows as $r) {
            $result[$r['name']] = $r['sql'];
        }
        return $result;
    }

    // -------------------------------------------------------------------------
    // Парсинг SQL-структур
    // -------------------------------------------------------------------------

    /**
     * Разбивает SQL на отдельные statements по символу ';',
     * игнорируя ';' внутри скобок, строковых литералов и BEGIN...END блоков.
     */
    public function splitStatements(string $sql): array
    {
        $statements = [];
        $current    = '';
        $depth      = 0;    // глубина скобок
        $beginDepth = 0;    // глубина BEGIN...END (для триггеров)
        $len        = strlen($sql);
        $inStr      = false;
        $strChar    = '';

        for ($i = 0; $i < $len; $i++) {
            $ch = $sql[$i];

            if ($inStr) {
                $current .= $ch;
                if ($ch === $strChar) {
                    // Проверяем на экранирование удвоением
                    if (isset($sql[$i + 1]) && $sql[$i + 1] === $strChar) {
                        $current .= $sql[++$i];
                    } else {
                        $inStr = false;
                    }
                }
                continue;
            }

            if ($ch === "'" || $ch === '"') {
                $inStr   = true;
                $strChar = $ch;
                $current .= $ch;
                continue;
            }

            if ($ch === '(') { $depth++; $current .= $ch; continue; }
            if ($ch === ')') { $depth--; $current .= $ch; continue; }

            // Проверяем ключевые слова BEGIN и END для триггеров
            if ($depth === 0 && !$inStr) {
                $upper5 = strtoupper(substr($sql, $i, 5));
                $upper3 = strtoupper(substr($sql, $i, 3));
                $prevChar = $i > 0 ? $sql[$i - 1] : ' ';
                $nextChar = isset($sql[$i + 5]) ? $sql[$i + 5] : ' ';
                $nextChar3 = isset($sql[$i + 3]) ? $sql[$i + 3] : ' ';

                if ($upper5 === 'BEGIN' && (ctype_space($prevChar) || $prevChar === '') &&
                    (ctype_space($nextChar) || $nextChar === ';' || $nextChar === '')) {
                    $beginDepth++;
                } elseif ($upper3 === 'END' && $beginDepth > 0 &&
                    (ctype_space($prevChar) || $prevChar === '') &&
                    (ctype_space($nextChar3) || $nextChar3 === ';' || $nextChar3 === '')) {
                    $beginDepth--;
                }
            }

            if ($ch === ';' && $depth === 0 && $beginDepth === 0) {
                $statements[] = trim($current);
                $current = '';
                continue;
            }

            $current .= $ch;
        }
        if (trim($current) !== '') {
            $statements[] = trim($current);
        }

        return $statements;
    }

    /**
     * Разбирает столбцы из CREATE TABLE.
     * Возвращает массив [ ['name'=>..., 'type'=>..., 'notnull'=>..., 'default'=>..., 'pk'=>...] ]
     */
    public function parseColumns(string $createSQL): array
    {
        // Извлекаем содержимое скобок верхнего уровня
        $body = $this->extractTableBody($createSQL);
        if ($body === null) return [];

        $parts = $this->splitByCommaTopLevel($body);
        $cols  = [];

        foreach ($parts as $part) {
            $part = trim($part);
            if ($part === '') continue;

            $upper = strtoupper($part);

            // Пропускаем табличные ограничения
            if (preg_match('/^(PRIMARY\s+KEY|UNIQUE|CHECK|FOREIGN\s+KEY)/i', $part)) continue;

            $col = $this->parseColumnDef($part);
            if ($col) $cols[] = $col;
        }

        return $cols;
    }

    /**
     * Парсит определение одного столбца.
     */
    public function parseColumnDef(string $def): ?array
    {
        $def = trim($def);

        // Имя столбца (с кавычками или без)
        if (preg_match('/^["`\[]?(\w+)["`\]]?\s+(.*)$/si', $def, $m)) {
            $name = $m[1];
            $rest = trim($m[2]);
        } else {
            return null;
        }

        // Тип
        $type    = '';
        $notnull = 0;
        $default = null;
        $pk      = 0;

        // Извлекаем тип: слово(а) до ключевых слов
        if (preg_match('/^([A-Za-z_][A-Za-z0-9_ ]*?)(?:\s*\([\d,\s]+\))?\s*(?:PRIMARY|NOT|DEFAULT|UNIQUE|REFERENCES|CHECK|$)/si', $rest, $tm)) {
            $type = strtoupper(trim($tm[1]));
        } elseif (preg_match('/^([A-Za-z_][A-Za-z0-9_ ]*?)(?:\s*\([\d,\s]+\))?\s*$/si', $rest, $tm)) {
            $type = strtoupper(trim($tm[1]));
        }

        if (stripos($rest, 'NOT NULL') !== false) $notnull = 1;
        if (stripos($rest, 'PRIMARY KEY') !== false) $pk = 1;

        // DEFAULT — значение может быть в скобках или без (с пробелом или без)
        if (preg_match('/DEFAULT\s*(\(([^)]*)\)|\'[^\']*\'|\S+)/si', $rest, $dm)) {
            $default = $dm[1];
        }

        return [
            'name'    => $name,
            'type'    => $type,
            'notnull' => $notnull,
            'default' => $default,
            'pk'      => $pk,
        ];
    }

    /**
     * Сравнивает столбец эталона с существующим.
     */
    public function columnsMatch(array $ref, array $existing): bool
    {
        if ($this->normalizeType($ref['type']) !== $this->normalizeType($existing['type'])) return false;
        if ($ref['notnull'] !== $existing['notnull']) return false;
        if (!$this->defaultsMatch($ref['default'], $existing['default'])) return false;
        return true;
    }

    /**
     * Нормализует тип SQLite для сравнения.
     */
    public function normalizeType(string $type): string
    {
        $type = strtoupper(trim($type));
        // Убираем размерность: INT(1) -> INT
        $type = preg_replace('/\s*\(\s*\d+\s*\)/', '', $type);
        return trim($type);
    }

    /**
     * Сравнивает значения DEFAULT.
     */
    public function defaultsMatch(?string $ref, ?string $existing): bool
    {
        $norm = function (?string $v): string {
            if ($v === null) return '';
            $v = trim($v);
            // Убираем внешние скобки: (0) -> 0
            while (strlen($v) >= 2 && $v[0] === '(' && substr($v, -1) === ')') {
                $inner = substr($v, 1, -1);
                if ($this->isBalanced($inner)) {
                    $v = trim($inner);
                } else {
                    break;
                }
            }
            // Убираем кавычки: 'abc' -> abc
            if (strlen($v) >= 2 && $v[0] === "'" && substr($v, -1) === "'") {
                $v = substr($v, 1, -1);
            }
            return strtolower($v);
        };

        return $norm($ref) === $norm($existing);
    }

    /**
     * Проверяет что скобки в строке сбалансированы.
     */
    private function isBalanced(string $s): bool
    {
        $depth = 0;
        foreach (str_split($s) as $ch) {
            if ($ch === '(') $depth++;
            if ($ch === ')') $depth--;
            if ($depth < 0) return false;
        }
        return $depth === 0;
    }

    /**
     * Строит строку определения столбца для ALTER TABLE ADD COLUMN.
     */
    private function buildColumnDef(array $col): string
    {
        $def = '"' . $col['name'] . '" ' . $col['type'];
        if ($col['notnull']) $def .= ' NOT NULL';
        if ($col['default'] !== null) $def .= ' DEFAULT ' . $col['default'];
        return $def;
    }

    /**
     * Парсит CREATE INDEX / CREATE UNIQUE INDEX.
     */
    public function parseIndexStatement(string $sql): ?array
    {
        if (preg_match(
            '/CREATE\s+(UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?["`\[]?(\w+)["`\]]?\s+ON\s+["`\[]?(\w+)["`\]]?\s*\(([^)]+)\)/si',
            $sql,
            $m
        )) {
            $cols = array_map('trim', explode(',', $m[4]));
            $cols = array_map(function ($c) {
                return trim($c, '"\' `[]');
            }, $cols);
            // Убираем ASC/DESC из имён столбцов
            $cols = array_map(function ($c) {
                return preg_replace('/\s+(ASC|DESC)$/i', '', $c);
            }, $cols);

            return [
                'name'    => $m[2],
                'unique'  => (int)!empty($m[1]),
                'table'   => $m[3],
                'columns' => $cols,
                'sql'     => $sql,
            ];
        }
        return null;
    }

    /**
     * Сравнивает два индекса.
     */
    public function indexesMatch(array $ref, array $existing): bool
    {
        if ($ref['unique'] !== $existing['unique']) return false;
        if ($ref['columns'] !== $existing['columns']) return false;
        return true;
    }

    // -------------------------------------------------------------------------
    // Вспомогательные методы парсинга
    // -------------------------------------------------------------------------

    private function extractCreateTableName(string $sql): ?string
    {
        if (preg_match('/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?["`\[]?(\w+)["`\]]?/si', $sql, $m)) {
            return $m[1];
        }
        return null;
    }

    private function extractObjectName(string $sql, string $keyword): ?string
    {
        if (preg_match('/CREATE\s+(?:TEMP\s+|TEMPORARY\s+)?' . $keyword . '\s+(?:IF\s+NOT\s+EXISTS\s+)?["`\[]?(\w+)["`\]]?/si', $sql, $m)) {
            return $m[1];
        }
        return null;
    }

    private function extractInsertTableName(string $sql): ?string
    {
        if (preg_match('/INSERT\s+(?:OR\s+\w+\s+)?INTO\s+["`\[]?(\w+)["`\]]?/si', $sql, $m)) {
            return $m[1];
        }
        return null;
    }

    /**
     * Извлекает содержимое скобок верхнего уровня CREATE TABLE (...).
     */
    public function extractTableBody(string $sql): ?string
    {
        $start = strpos($sql, '(');
        if ($start === false) return null;

        $depth  = 0;
        $len    = strlen($sql);
        $result = '';

        for ($i = $start; $i < $len; $i++) {
            $ch = $sql[$i];
            if ($ch === '(') {
                if ($depth > 0) $result .= $ch;
                $depth++;
            } elseif ($ch === ')') {
                $depth--;
                if ($depth === 0) break;
                $result .= $ch;
            } else {
                if ($depth > 0) $result .= $ch;
            }
        }

        return $result;
    }

    /**
     * Разбивает строку по запятым, игнорируя запятые внутри скобок.
     */
    public function splitByCommaTopLevel(string $s): array
    {
        $parts  = [];
        $depth  = 0;
        $current = '';
        $len    = strlen($s);
        $inStr  = false;
        $strChar = '';

        for ($i = 0; $i < $len; $i++) {
            $ch = $s[$i];

            if ($inStr) {
                $current .= $ch;
                if ($ch === $strChar) $inStr = false;
                continue;
            }
            if ($ch === "'" || $ch === '"') {
                $inStr = true; $strChar = $ch; $current .= $ch; continue;
            }

            if ($ch === '(') { $depth++; $current .= $ch; continue; }
            if ($ch === ')') { $depth--; $current .= $ch; continue; }

            if ($ch === ',' && $depth === 0) {
                $parts[] = trim($current);
                $current = '';
                continue;
            }
            $current .= $ch;
        }
        if (trim($current) !== '') $parts[] = trim($current);

        return $parts;
    }

    /**
     * Нормализует SQL для сравнения (пробелы, регистр).
     */
    private function sqlNormEqual(string $a, string $b): bool
    {
        $norm = function (string $s): string {
            $s = strtolower(trim($s));
            $s = preg_replace('/\s+/', ' ', $s);
            return $s;
        };
        return $norm($a) === $norm($b);
    }

    // -------------------------------------------------------------------------
    // Вывод
    // -------------------------------------------------------------------------

    private function logVerbose(string $msg): void
    {
        if ($this->verbose) {
            echo $msg . PHP_EOL;
        }
    }

    private function printSummary(): void
    {
        $mode = $this->dryRun ? '[DRY-RUN]' : '[APPLIED]';
        echo PHP_EOL . "=== Результат актуализации БД {$mode} ===" . PHP_EOL;

        if (empty($this->changes) && empty($this->errors)) {
            echo "Изменений не требуется. БД соответствует эталону." . PHP_EOL;
            return;
        }

        if (!empty($this->changes)) {
            echo PHP_EOL . "Изменения (" . count($this->changes) . "):" . PHP_EOL;
            foreach ($this->changes as $ch) {
                echo "  + {$ch}" . PHP_EOL;
            }
        }

        if (!empty($this->errors)) {
            echo PHP_EOL . "ОШИБКИ (" . count($this->errors) . "):" . PHP_EOL;
            foreach ($this->errors as $err) {
                echo "  ! {$err}" . PHP_EOL;
            }
        }
    }
}

// =============================================================================
// CLI точка входа
// =============================================================================

if (PHP_SAPI === 'cli' && realpath($argv[0]) === realpath(__FILE__)) {
    $opts = getopt('', ['db:', 'sql:', 'dry-run', 'verbose']);

    $dbPath  = $opts['db']  ?? null;
    $sqlPath = $opts['sql'] ?? null;
    $dryRun  = isset($opts['dry-run']);
    $verbose = isset($opts['verbose']);

    if (!$dbPath || !$sqlPath) {
        echo "Использование: php db_actualizer.php --db=<путь к БД> --sql=<путь к SQL> [--dry-run] [--verbose]\n";
        exit(1);
    }

    if (!file_exists($sqlPath)) {
        echo "Файл не найден: {$sqlPath}\n";
        exit(1);
    }

    $actualizer = new DbActualizer($dbPath, $sqlPath, $dryRun, $verbose);
    $ok = $actualizer->run();
    exit($ok ? 0 : 1);
}
