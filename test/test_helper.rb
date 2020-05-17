require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"
require "pg"
require "shellwords"
require "tmpdir"
require "open3"

def connect(dbname)
  conn = PG::Connection.open(dbname: dbname)
  conn.exec("SET client_min_messages TO WARNING")
  conn.type_map_for_results = PG::BasicTypeMapForResults.new(conn)
  conn.exec(File.read("test/support/schema#{dbname[-1]}.sql"))
  conn
end

$conn1 = connect("pgsync_test1")
$conn2 = connect("pgsync_test2")
$conn3 = connect("pgsync_test3")

class Minitest::Test
  def verbose?
    ENV["VERBOSE"]
  end

  def run_command(command, dbs: false)
    command << " --from pgsync_test1 --to pgsync_test2" if dbs
    if verbose?
      puts
      puts "$ pgsync #{command}"
    end
    exe = File.expand_path("../exe/pgsync", __dir__)
    output, status = Open3.capture2e(exe, *Shellwords.split(command))
    puts output if verbose?
    [output, status]
  end

  def assert_works(command, dbs: false)
    output, status = run_command(command, dbs: dbs)
    assert status.success?, "Command failed"
    output
  end

  def assert_error(message, command, dbs: false)
    output, status = run_command(command, dbs: dbs)
    assert !status.success?
    assert_match message, output
  end

  def assert_prints(message, command, dbs: false)
    output, status = run_command(command, dbs: dbs)
    assert_match message, output
  end

  def truncate(conn, table)
    conn.exec("TRUNCATE #{quote_ident(table)} CASCADE")
  end

  def insert(conn, table, rows)
    return if rows.empty?

    keys = rows.flat_map { |r| r.keys }.uniq
    values = rows.map { |r| keys.map { |k| r[k] } }

    key_str = keys.map { |k| quote_ident(k) }.join(", ")
    params_str = values.size.times.map { |i| "(" + keys.size.times.map { |j| "$#{i * keys.size + j + 1}" }.join(", ") + ")" }.join(", ")
    insert_str = "INSERT INTO #{quote_ident(table)} (#{key_str}) VALUES #{params_str}"
    conn.exec_params(insert_str, values.flatten)
  end

  def quote_ident(ident)
    PG::Connection.quote_ident(ident)
  end
end
