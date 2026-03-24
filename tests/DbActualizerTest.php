<?php
/**
 * Тесты для DbActualizer
 * Запуск: php tests/DbActualizerTest.php
 */

declare(strict_types=1);

require_once __DIR__ . '/../db_actualizer.php';

// =============================================================================
// Минимальный тест-раннер
// =============================================================================

class TestRunner
{
    private $passed = 0;
    private $failed = 0;
    private $current = '';

    public function describe(string $name): void
    {
        $this->current = $name;
        echo PHP_EOL . "--- {$name}" . PHP_EOL;
    }

    public function it(string $label, callable $fn): void
    {
        try {
            $fn($this);
            $this->passed++;
            echo "  [PASS] {$label}" . PHP_EOL;
        } catch (AssertionError $e) {
            $this->failed++;
            echo "  [FAIL] {$label}" . PHP_EOL;
            echo "         " . $e->getMessage() . PHP_EOL;
        } catch (Throwable $e) {
            $this->failed++;
            echo "  [FAIL] {$label}" . PHP_EOL;
            echo "         Exception: " . $e->getMessage() . PHP_EOL;
            echo "         " . $e->getFile() . ':' . $e->getLine() . PHP_EOL;
        }
    }

    public function assert(bool $condition, string $message = ''): void
    {
        if (!$condition) throw new AssertionError($message ?: 'Assertion failed');
    }

    public function assertEqual($expected, $actual, string $message = ''): void
    {
        if ($expected !== $actual) {
            throw new AssertionError(
                ($message ? $message . ' ' : '') .
                "Expected: " . var_export($expected, true) .
                " Got: " . var_export($actual, true)
            );
        }
    }

    public function assertContains(string $needle, array $haystack, string $message = ''): void
    {
        if (!in_array($needle, $haystack, true)) {
            throw new AssertionError(
                ($message ? $message . ' ' : '') .
                "Expected array to contain: " . var_export($needle, true) .
                " Array: " . implode(', ', array_map(function ($v) { return var_export($v, true); }, $haystack))
            );
        }
    }

    public function assertNotContains(string $needle, array $haystack, string $message = ''): void
    {
        if (in_array($needle, $haystack, true)) {
            throw new AssertionError(
                ($message ? $message . ' ' : '') .
                "Expected array NOT to contain: " . var_export($needle, true)
            );
        }
    }

    public function summary(): void
    {
        echo PHP_EOL . str_repeat('=', 50) . PHP_EOL;
        echo "Итого: {$this->passed} пройдено, {$this->failed} провалено" . PHP_EOL;
        if ($this->failed > 0) exit(1);
    }
}

// =============================================================================
// Вспомогательные функции
// =============================================================================

function tmpDb(): string
{
    return tempnam(sys_get_temp_dir(), 'db_act_test_') . '.sqlite';
}

function tmpSql(string $sql): string
{
    $f = tempnam(sys_get_temp_dir(), 'db_act_sql_');
    file_put_contents($f, $sql);
    return $f;
}

/**
 * Создаёт актуализатор с временными файлами.
 * $initialSql — SQL для начального состояния БД (null = пустая БД).
 * $referenceSql — эталонный SQL.
 */
function makeActualizer(
    string $referenceSql,
    ?string $initialSql = null,
    bool $dryRun = false,
    bool $verbose = false
): array {
    $dbPath  = tmpDb();
    $sqlFile = tmpSql($referenceSql);

    if ($initialSql !== null) {
        $db = new PDO('sqlite:' . $dbPath);
        $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        // Используем splitStatements чтобы корректно обрабатывать BEGIN..END в триггерах
        $tmpAct = new DbActualizer($dbPath, tmpSql(''), false, false);
        foreach ($tmpAct->splitStatements($initialSql) as $stmt) {
            $stmt = trim($stmt);
            if ($stmt !== '') $db->exec($stmt);
        }
        unset($db);
    }

    $act = new DbActualizer($dbPath, $sqlFile, $dryRun, $verbose);
    return [$act, $dbPath, $sqlFile];
}

function cleanup(string ...$files): void
{
    foreach ($files as $f) {
        if (file_exists($f)) @unlink($f);
    }
}

function openDb(string $path): PDO
{
    $db = new PDO('sqlite:' . $path);
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    return $db;
}

function tableExists(PDO $db, string $table): bool
{
    $r = $db->query("SELECT name FROM sqlite_master WHERE type='table' AND name='{$table}'")->fetch();
    return $r !== false;
}

function columnExists(PDO $db, string $table, string $column): bool
{
    $cols = $db->query("PRAGMA table_info(\"{$table}\")")->fetchAll(PDO::FETCH_ASSOC);
    foreach ($cols as $c) {
        if ($c['name'] === $column) return true;
    }
    return false;
}

function getColumnInfo(PDO $db, string $table, string $column): ?array
{
    $cols = $db->query("PRAGMA table_info(\"{$table}\")")->fetchAll(PDO::FETCH_ASSOC);
    foreach ($cols as $c) {
        if ($c['name'] === $column) return $c;
    }
    return null;
}

function indexExists(PDO $db, string $index): bool
{
    $r = $db->query("SELECT name FROM sqlite_master WHERE type='index' AND name='{$index}'")->fetch();
    return $r !== false;
}

// =============================================================================
// ТЕСТЫ
// =============================================================================

$t = new TestRunner();

// -------------------------------------------------------------------------
$t->describe('Парсинг SQL');
// -------------------------------------------------------------------------

$t->it('splitStatements: простой случай', function (TestRunner $t) {
    $act = new DbActualizer(':memory:', tmpSql(''), false, false);
    // Используем рефлексию через публичный метод
    $stmts = $act->splitStatements("CREATE TABLE a (id INT); CREATE TABLE b (id INT);");
    $t->assertEqual(2, count($stmts));
    $t->assert(strpos($stmts[0], 'CREATE TABLE a') !== false);
    $t->assert(strpos($stmts[1], 'CREATE TABLE b') !== false);
});

$t->it('splitStatements: скобки внутри statement', function (TestRunner $t) {
    $act = new DbActualizer(':memory:', tmpSql(''), false, false);
    $stmts = $act->splitStatements("CREATE TABLE a (id INT, val DOUBLE DEFAULT(0)); INSERT INTO a VALUES (1, 2.0);");
    $t->assertEqual(2, count($stmts));
});

$t->it('splitStatements: строковый литерал с точкой с запятой', function (TestRunner $t) {
    $act = new DbActualizer(':memory:', tmpSql(''), false, false);
    $stmts = $act->splitStatements("INSERT INTO t (v) VALUES ('a;b');");
    $t->assertEqual(1, count($stmts));
});

$t->it('parseColumnDef: простой тип без умолчания', function (TestRunner $t) {
    $act = new DbActualizer(':memory:', tmpSql(''), false, false);
    $col = $act->parseColumnDef('id INT PRIMARY KEY');
    $t->assertEqual('INT', $col['type']);
    $t->assertEqual('id', $col['name']);
    $t->assertEqual(1, $col['pk']);
    $t->assertEqual(null, $col['default']);
});

$t->it('parseColumnDef: DEFAULT со скобками', function (TestRunner $t) {
    $act = new DbActualizer(':memory:', tmpSql(''), false, false);
    $col = $act->parseColumnDef('price DOUBLE DEFAULT(0)');
    $t->assertEqual('DOUBLE', $col['type']);
    $t->assert($col['default'] !== null, 'default не должен быть null');
    $t->assertEqual('0', trim((string)$col['default'], '()'));
});

$t->it('parseColumnDef: DEFAULT со строкой', function (TestRunner $t) {
    $act = new DbActualizer(':memory:', tmpSql(''), false, false);
    $col = $act->parseColumnDef("title VARCHAR DEFAULT ''");
    $t->assertEqual('VARCHAR', $col['type']);
    $t->assertEqual("''", $col['default']);
});

$t->it('parseColumnDef: NOT NULL', function (TestRunner $t) {
    $act = new DbActualizer(':memory:', tmpSql(''), false, false);
    $col = $act->parseColumnDef('name VARCHAR NOT NULL');
    $t->assertEqual(1, $col['notnull']);
});

$t->it('parseColumns: несколько столбцов', function (TestRunner $t) {
    $act = new DbActualizer(':memory:', tmpSql(''), false, false);
    $cols = $act->parseColumns("CREATE TABLE t (id INT PRIMARY KEY, title VARCHAR, price DOUBLE DEFAULT(0))");
    $t->assertEqual(3, count($cols));
    $t->assertEqual('id', $cols[0]['name']);
    $t->assertEqual('title', $cols[1]['name']);
    $t->assertEqual('price', $cols[2]['name']);
});

$t->it('parseColumns: пропускает табличные ограничения PRIMARY KEY', function (TestRunner $t) {
    $act = new DbActualizer(':memory:', tmpSql(''), false, false);
    $cols = $act->parseColumns("CREATE TABLE t (id INT, name VARCHAR, PRIMARY KEY(id))");
    $t->assertEqual(2, count($cols));
});

$t->it('parseIndexStatement: обычный индекс', function (TestRunner $t) {
    $act = new DbActualizer(':memory:', tmpSql(''), false, false);
    $idx = $act->parseIndexStatement("CREATE INDEX my_idx ON products (category_id)");
    $t->assertEqual('my_idx', $idx['name']);
    $t->assertEqual('products', $idx['table']);
    $t->assertEqual(['category_id'], $idx['columns']);
    $t->assertEqual(0, $idx['unique']);
});

$t->it('parseIndexStatement: уникальный индекс', function (TestRunner $t) {
    $act = new DbActualizer(':memory:', tmpSql(''), false, false);
    $idx = $act->parseIndexStatement("CREATE UNIQUE INDEX my_idx ON tbl (a, b)");
    $t->assertEqual(1, $idx['unique']);
    $t->assertEqual(['a', 'b'], $idx['columns']);
});

$t->it('defaultsMatch: эквивалентные формы', function (TestRunner $t) {
    $act = new DbActualizer(':memory:', tmpSql(''), false, false);
    $t->assert($act->defaultsMatch('(0)', '0'));
    $t->assert($act->defaultsMatch("''", null));  // пустая строка = null
    $t->assert($act->defaultsMatch('0', '(0)'));
    $t->assert($act->defaultsMatch("'hello'", 'hello'));
    $t->assert(!$act->defaultsMatch('1', '0'));
    $t->assert(!$act->defaultsMatch("'abc'", "'xyz'"));
});

$t->it('normalizeType: убирает размерность', function (TestRunner $t) {
    $act = new DbActualizer(':memory:', tmpSql(''), false, false);
    $t->assertEqual('INT', $act->normalizeType('INT(1)'));
    $t->assertEqual('VARCHAR', $act->normalizeType('VARCHAR'));
    $t->assertEqual('DOUBLE', $act->normalizeType('double'));
});

// -------------------------------------------------------------------------
$t->describe('Создание отсутствующих таблиц');
// -------------------------------------------------------------------------

$t->it('создаёт таблицу которой нет в БД', function (TestRunner $t) {
    $ref = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, '');
    $act->run();
    $db = openDb($dbPath);
    $t->assert(tableExists($db, 'users'), 'Таблица users должна быть создана');
    cleanup($dbPath, $sqlFile);
});

$t->it('создаёт несколько таблиц', function (TestRunner $t) {
    $ref = "CREATE TABLE a (id INT); CREATE TABLE b (id INT);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, '');
    $act->run();
    $db = openDb($dbPath);
    $t->assert(tableExists($db, 'a'));
    $t->assert(tableExists($db, 'b'));
    cleanup($dbPath, $sqlFile);
});

$t->it('не изменяет ничего если БД соответствует эталону', function (TestRunner $t) {
    $ref = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $ref);
    $act->run();
    $changes = $act->getChanges();
    $t->assertEqual([], $changes, 'Изменений быть не должно');
    cleanup($dbPath, $sqlFile);
});

// -------------------------------------------------------------------------
$t->describe('Добавление столбцов');
// -------------------------------------------------------------------------

$t->it('добавляет отсутствующий столбец', function (TestRunner $t) {
    $initial = "CREATE TABLE products (id INT PRIMARY KEY, title VARCHAR);";
    $ref     = "CREATE TABLE products (id INT PRIMARY KEY, title VARCHAR, price DOUBLE DEFAULT(0));";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $t->assert(columnExists($db, 'products', 'price'), 'Столбец price должен быть добавлен');
    cleanup($dbPath, $sqlFile);
});

$t->it('добавляет столбец с DEFAULT', function (TestRunner $t) {
    $initial = "CREATE TABLE t (id INT);";
    $ref     = "CREATE TABLE t (id INT, flag INT DEFAULT(0));";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $col = getColumnInfo($db, 't', 'flag');
    $t->assert($col !== null, 'Столбец flag должен существовать');
    cleanup($dbPath, $sqlFile);
});

$t->it('добавляет несколько столбцов', function (TestRunner $t) {
    $initial = "CREATE TABLE t (id INT);";
    $ref     = "CREATE TABLE t (id INT, a VARCHAR, b INT DEFAULT(1), c DOUBLE);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $t->assert(columnExists($db, 't', 'a'));
    $t->assert(columnExists($db, 't', 'b'));
    $t->assert(columnExists($db, 't', 'c'));
    cleanup($dbPath, $sqlFile);
});

// -------------------------------------------------------------------------
$t->describe('Удаление лишних столбцов (пересоздание таблицы)');
// -------------------------------------------------------------------------

$t->it('удаляет лишний столбец через пересоздание', function (TestRunner $t) {
    $initial = "CREATE TABLE t (id INT, name VARCHAR, extra TEXT);";
    $ref     = "CREATE TABLE t (id INT, name VARCHAR);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $t->assert(!columnExists($db, 't', 'extra'), 'Столбец extra должен быть удалён');
    $t->assert(columnExists($db, 't', 'id'));
    $t->assert(columnExists($db, 't', 'name'));
    cleanup($dbPath, $sqlFile);
});

$t->it('сохраняет данные при удалении лишнего столбца', function (TestRunner $t) {
    $initial = "CREATE TABLE t (id INT, name VARCHAR, extra TEXT); INSERT INTO t VALUES (1, 'Alice', 'junk');";
    $ref     = "CREATE TABLE t (id INT, name VARCHAR);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $row = $db->query("SELECT * FROM t WHERE id=1")->fetch(PDO::FETCH_ASSOC);
    $t->assert($row !== false, 'Запись должна сохраниться');
    $t->assertEqual('Alice', $row['name']);
    cleanup($dbPath, $sqlFile);
});

// -------------------------------------------------------------------------
$t->describe('Изменение типа столбца (пересоздание таблицы)');
// -------------------------------------------------------------------------

$t->it('изменяет тип столбца', function (TestRunner $t) {
    $initial = "CREATE TABLE t (id INT, val VARCHAR);";
    $ref     = "CREATE TABLE t (id INT, val DOUBLE);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $col = getColumnInfo($db, 't', 'val');
    $t->assertEqual('DOUBLE', strtoupper($col['type']));
    cleanup($dbPath, $sqlFile);
});

$t->it('изменяет DEFAULT столбца', function (TestRunner $t) {
    $initial = "CREATE TABLE t (id INT, flag INT DEFAULT(0));";
    $ref     = "CREATE TABLE t (id INT, flag INT DEFAULT(1));";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $col = getColumnInfo($db, 't', 'flag');
    // После пересоздания DEFAULT должен быть 1
    $t->assert(
        in_array($col['dflt_value'], ['1', '(1)'], true),
        "dflt_value должен быть 1, получено: " . var_export($col['dflt_value'], true)
    );
    cleanup($dbPath, $sqlFile);
});

$t->it('сохраняет данные при изменении типа', function (TestRunner $t) {
    $initial = "CREATE TABLE t (id INT, name VARCHAR); INSERT INTO t VALUES (42, 'Bob');";
    $ref     = "CREATE TABLE t (id INT, name TEXT);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $row = $db->query("SELECT * FROM t WHERE id=42")->fetch(PDO::FETCH_ASSOC);
    $t->assert($row !== false);
    $t->assertEqual('Bob', $row['name']);
    cleanup($dbPath, $sqlFile);
});

// -------------------------------------------------------------------------
$t->describe('Удаление лишних таблиц');
// -------------------------------------------------------------------------

$t->it('удаляет таблицу которой нет в эталоне', function (TestRunner $t) {
    $initial = "CREATE TABLE keep_me (id INT); CREATE TABLE delete_me (id INT);";
    $ref     = "CREATE TABLE keep_me (id INT);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $t->assert(!tableExists($db, 'delete_me'), 'Таблица delete_me должна быть удалена');
    $t->assert(tableExists($db, 'keep_me'), 'Таблица keep_me должна остаться');
    cleanup($dbPath, $sqlFile);
});

// -------------------------------------------------------------------------
$t->describe('Индексы');
// -------------------------------------------------------------------------

$t->it('создаёт отсутствующий индекс', function (TestRunner $t) {
    $initial = "CREATE TABLE products (id INT, category_id INT);";
    $ref     = "CREATE TABLE products (id INT, category_id INT);\nCREATE INDEX cat_idx ON products (category_id);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $t->assert(indexExists($db, 'cat_idx'), 'Индекс cat_idx должен быть создан');
    cleanup($dbPath, $sqlFile);
});

$t->it('создаёт уникальный индекс', function (TestRunner $t) {
    $initial = "CREATE TABLE t (id INT, email VARCHAR);";
    $ref     = "CREATE TABLE t (id INT, email VARCHAR);\nCREATE UNIQUE INDEX email_idx ON t (email);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $t->assert(indexExists($db, 'email_idx'), 'Индекс email_idx должен существовать');
    $list = $db->query("PRAGMA index_list(\"t\")")->fetchAll(PDO::FETCH_ASSOC);
    $found = null;
    foreach ($list as $row) { if ($row['name'] === 'email_idx') { $found = $row; break; } }
    $t->assert($found !== null, 'Индекс должен быть в PRAGMA index_list');
    $t->assertEqual('1', (string)$found['unique'], 'Индекс должен быть уникальным');
    cleanup($dbPath, $sqlFile);
});

$t->it('удаляет лишний индекс', function (TestRunner $t) {
    $initial = "CREATE TABLE t (id INT, a INT);\nCREATE INDEX old_idx ON t (a);";
    $ref     = "CREATE TABLE t (id INT, a INT);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $t->assert(!indexExists($db, 'old_idx'), 'Индекс old_idx должен быть удалён');
    cleanup($dbPath, $sqlFile);
});

$t->it('пересоздаёт изменившийся индекс (unique -> non-unique)', function (TestRunner $t) {
    $initial = "CREATE TABLE t (id INT, a INT);\nCREATE UNIQUE INDEX my_idx ON t (a);";
    $ref     = "CREATE TABLE t (id INT, a INT);\nCREATE INDEX my_idx ON t (a);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $t->assert(indexExists($db, 'my_idx'), 'Индекс my_idx должен существовать');
    $list = $db->query("PRAGMA index_list(\"t\")")->fetchAll(PDO::FETCH_ASSOC);
    $found = null;
    foreach ($list as $row) { if ($row['name'] === 'my_idx') { $found = $row; break; } }
    $t->assert($found !== null);
    $t->assertEqual('0', (string)$found['unique'], 'Индекс должен стать non-unique');
    cleanup($dbPath, $sqlFile);
});

$t->it('пересоздаёт индекс при изменении столбцов', function (TestRunner $t) {
    $initial = "CREATE TABLE t (id INT, a INT, b INT);\nCREATE INDEX my_idx ON t (a);";
    $ref     = "CREATE TABLE t (id INT, a INT, b INT);\nCREATE INDEX my_idx ON t (a, b);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $cols = $db->query("PRAGMA index_info(\"my_idx\")")->fetchAll(PDO::FETCH_ASSOC);
    $names = array_column($cols, 'name');
    $t->assert(in_array('b', $names), 'Индекс должен включать столбец b');
    cleanup($dbPath, $sqlFile);
});

// -------------------------------------------------------------------------
$t->describe('Режим dry-run');
// -------------------------------------------------------------------------

$t->it('dry-run не вносит изменений в БД', function (TestRunner $t) {
    $initial = "CREATE TABLE t (id INT);";
    $ref     = "CREATE TABLE t (id INT, new_col VARCHAR);\nCREATE TABLE new_table (id INT);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial, true);
    $act->run();
    $db = openDb($dbPath);
    $t->assert(!tableExists($db, 'new_table'), 'new_table не должна быть создана в dry-run');
    $t->assert(!columnExists($db, 't', 'new_col'), 'new_col не должна быть добавлена в dry-run');
    cleanup($dbPath, $sqlFile);
});

$t->it('dry-run сообщает о найденных различиях', function (TestRunner $t) {
    $initial = "CREATE TABLE t (id INT);";
    $ref     = "CREATE TABLE t (id INT, extra VARCHAR); CREATE TABLE new_tbl (x INT);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial, true);
    $act->run();
    $changes = $act->getChanges();
    $t->assert(count($changes) > 0, 'dry-run должен возвращать список изменений');
    cleanup($dbPath, $sqlFile);
});

$t->it('dry-run возвращает пустой список если изменений нет', function (TestRunner $t) {
    $ref = "CREATE TABLE t (id INT, name VARCHAR);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $ref, true);
    $act->run();
    $changes = $act->getChanges();
    $t->assertEqual([], $changes, 'Изменений не должно быть');
    cleanup($dbPath, $sqlFile);
});

// -------------------------------------------------------------------------
$t->describe('Views');
// -------------------------------------------------------------------------

$t->it('создаёт отсутствующий view', function (TestRunner $t) {
    $ref = "CREATE TABLE t (id INT, val INT);\nCREATE VIEW v_total AS SELECT sum(val) as total FROM t;";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, "CREATE TABLE t (id INT, val INT);");
    $act->run();
    $db = openDb($dbPath);
    $r = $db->query("SELECT name FROM sqlite_master WHERE type='view' AND name='v_total'")->fetch();
    $t->assert($r !== false, 'View v_total должен быть создан');
    cleanup($dbPath, $sqlFile);
});

$t->it('удаляет лишний view', function (TestRunner $t) {
    $initial = "CREATE TABLE t (id INT); CREATE VIEW old_view AS SELECT id FROM t;";
    $ref     = "CREATE TABLE t (id INT);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $r = $db->query("SELECT name FROM sqlite_master WHERE type='view' AND name='old_view'")->fetch();
    $t->assert($r === false, 'View old_view должен быть удалён');
    cleanup($dbPath, $sqlFile);
});

// -------------------------------------------------------------------------
$t->describe('Triggers');
// -------------------------------------------------------------------------

$t->it('создаёт отсутствующий триггер', function (TestRunner $t) {
    $initial = "CREATE TABLE t (id INT, updated_at INT);";
    $ref = "CREATE TABLE t (id INT, updated_at INT);\n" .
           "CREATE TRIGGER upd_time AFTER UPDATE ON t BEGIN UPDATE t SET updated_at=strftime('%s','now') WHERE id=NEW.id; END;";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $r = $db->query("SELECT name FROM sqlite_master WHERE type='trigger' AND name='upd_time'")->fetch();
    $t->assert($r !== false, 'Триггер upd_time должен быть создан');
    cleanup($dbPath, $sqlFile);
});

$t->it('удаляет лишний триггер', function (TestRunner $t) {
    $initial = "CREATE TABLE t (id INT);\n" .
               "CREATE TRIGGER old_trig AFTER INSERT ON t BEGIN SELECT 1; END;";
    $ref = "CREATE TABLE t (id INT);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $r = $db->query("SELECT name FROM sqlite_master WHERE type='trigger' AND name='old_trig'")->fetch();
    $t->assert($r === false, 'Триггер old_trig должен быть удалён');
    cleanup($dbPath, $sqlFile);
});

// -------------------------------------------------------------------------
$t->describe('Комплексные сценарии');
// -------------------------------------------------------------------------

$t->it('одновременно: добавить таблицу, удалить таблицу, добавить столбец, создать индекс', function (TestRunner $t) {
    $initial = "CREATE TABLE keep (id INT, name VARCHAR);\nCREATE TABLE remove_me (id INT);";
    $ref     = "CREATE TABLE keep (id INT, name VARCHAR, score INT DEFAULT(0));\n" .
               "CREATE TABLE new_tbl (x INT, y INT);\n" .
               "CREATE INDEX keep_name ON keep (name);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $t->assert(tableExists($db, 'new_tbl'), 'new_tbl создана');
    $t->assert(!tableExists($db, 'remove_me'), 'remove_me удалена');
    $t->assert(columnExists($db, 'keep', 'score'), 'столбец score добавлен');
    $t->assert(indexExists($db, 'keep_name'), 'индекс keep_name создан');
    cleanup($dbPath, $sqlFile);
});

$t->it('пересоздание таблицы восстанавливает индексы', function (TestRunner $t) {
    // При пересоздании индексы не должны исчезнуть — они синхронизируются после
    $initial = "CREATE TABLE t (id INT, a VARCHAR, b INT);";
    $ref     = "CREATE TABLE t (id INT, a TEXT, b INT);\n" .  // изменили тип a
               "CREATE INDEX t_a ON t (a);\n" .
               "CREATE INDEX t_b ON t (b);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $t->assert(indexExists($db, 't_a'), 'индекс t_a должен быть создан');
    $t->assert(indexExists($db, 't_b'), 'индекс t_b должен быть создан');
    cleanup($dbPath, $sqlFile);
});

$t->it('реальный db.sql: пустая БД полностью инициализируется', function (TestRunner $t) {
    $sqlFile = __DIR__ . '/../db.sql';
    if (!file_exists($sqlFile)) {
        echo "    [SKIP] db.sql не найден" . PHP_EOL;
        return;
    }
    $dbPath = tmpDb();
    $act = new DbActualizer($dbPath, $sqlFile, false, false);
    $ok = $act->run();
    $t->assert($ok, 'Ошибок быть не должно: ' . implode('; ', $act->getErrors()));

    // Проверяем несколько ключевых таблиц
    $db = openDb($dbPath);
    foreach (['products', 'receipts', 'users', 'settings', 'paymentmethods'] as $tbl) {
        $t->assert(tableExists($db, $tbl), "Таблица {$tbl} должна быть создана");
    }
    cleanup($dbPath);
});

$t->it('реальный db.sql: повторный запуск не вносит изменений', function (TestRunner $t) {
    $sqlFile = __DIR__ . '/../db.sql';
    if (!file_exists($sqlFile)) {
        echo "    [SKIP] db.sql не найден" . PHP_EOL;
        return;
    }
    $dbPath = tmpDb();

    // Первый прогон
    $act1 = new DbActualizer($dbPath, $sqlFile, false, false);
    $act1->run();

    // Второй прогон — изменений не должно быть
    $act2 = new DbActualizer($dbPath, $sqlFile, false, false);
    $act2->run();
    $t->assertEqual([], $act2->getChanges(), 'Повторный запуск не должен вносить изменений');
    $t->assertEqual([], $act2->getErrors(), 'Ошибок быть не должно');
    cleanup($dbPath);
});

// -------------------------------------------------------------------------
$t->describe('Граничные случаи');
// -------------------------------------------------------------------------

$t->it('таблица с INTEGER PRIMARY KEY AUTOINCREMENT', function (TestRunner $t) {
    $ref = "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, '');
    $act->run();
    $db = openDb($dbPath);
    $t->assert(tableExists($db, 't'));
    // Проверяем что AUTOINCREMENT работает
    $db->exec("INSERT INTO t (name) VALUES ('test')");
    $id = $db->lastInsertId();
    $t->assert($id > 0, 'AUTOINCREMENT должен работать');
    cleanup($dbPath, $sqlFile);
});

$t->it('индекс с несколькими столбцами', function (TestRunner $t) {
    $initial = "CREATE TABLE t (a INT, b INT, c INT);";
    $ref     = "CREATE TABLE t (a INT, b INT, c INT);\nCREATE UNIQUE INDEX abc_idx ON t (a, b, c);";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, $initial);
    $act->run();
    $db = openDb($dbPath);
    $t->assert(indexExists($db, 'abc_idx'));
    $cols = $db->query("PRAGMA index_info(\"abc_idx\")")->fetchAll(PDO::FETCH_ASSOC);
    $t->assertEqual(3, count($cols));
    cleanup($dbPath, $sqlFile);
});

$t->it('пустой эталон — удаляет все таблицы', function (TestRunner $t) {
    $initial = "CREATE TABLE a (id INT); CREATE TABLE b (id INT);";
    [$act, $dbPath, $sqlFile] = makeActualizer('', $initial);
    $act->run();
    $db = openDb($dbPath);
    $t->assert(!tableExists($db, 'a'), 'Таблица a должна быть удалена');
    $t->assert(!tableExists($db, 'b'), 'Таблица b должна быть удалена');
    cleanup($dbPath, $sqlFile);
});

$t->it('столбец с именем в кавычках в CREATE TABLE', function (TestRunner $t) {
    $ref = 'CREATE TABLE t ("id" INT PRIMARY KEY, "value" TEXT DEFAULT \'\');';
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, '');
    $act->run();
    $db = openDb($dbPath);
    $t->assert(tableExists($db, 't'));
    $t->assert(columnExists($db, 't', 'id'));
    $t->assert(columnExists($db, 't', 'value'));
    cleanup($dbPath, $sqlFile);
});

$t->it('таблица с данными: INSERT initial data выполняется', function (TestRunner $t) {
    $ref = "CREATE TABLE settings (name VARCHAR UNIQUE, value TEXT);\n" .
           "INSERT INTO settings (name, value) VALUES ('key1', 'val1'), ('key2', 'val2');";
    [$act, $dbPath, $sqlFile] = makeActualizer($ref, '');
    $act->run();
    $db = openDb($dbPath);
    $count = $db->query("SELECT COUNT(*) FROM settings")->fetchColumn();
    $t->assertEqual('2', (string)$count, 'Должно быть 2 записи');
    cleanup($dbPath, $sqlFile);
});

$t->summary();
