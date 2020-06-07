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

def conn1
  @conn1 ||= connect("pgsync_test1")
end

def conn2
  @conn2 ||= connect("pgsync_test2")
end

def conn3
  @conn3 ||= connect("pgsync_test3")
end

[conn1, conn2, conn3] # setup schema

class Minitest::Test
  def verbose?
    ENV["VERBOSE"]
  end

  # shelling out for each test is slower
  # but it prevents forking from messing up connections
  def run_command(command, config: false)
    command << " --config test/support/config.yml" if config
    if verbose?
      puts
      puts "$ pgsync #{command}"
    end
    exe = File.expand_path("../exe/pgsync", __dir__)
    output, status = Open3.capture2e(exe, *Shellwords.split(command))
    puts output if verbose?
    [output, status]
  end

  def assert_works(command, **options)
    output, status = run_command(command, **options)
    assert status.success?, "Command failed"
    output
  end

  def assert_error(message, command, **options)
    output, status = run_command(command, **options)
    assert !status.success?
    assert_match message, output
  end

  def assert_prints(message, command, **options)
    output, status = run_command(command, **options)
    assert_match message, output
  end

  def truncate(conn, table)
    conn.exec("TRUNCATE #{quote_ident(table)} CASCADE")
  end

  def truncate_tables(tables)
    [conn1, conn2].each do |conn|
      tables.each do |table|
        truncate(conn, table)
      end
    end
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

  def assert_result(command, source, dest, expected, table = "posts")
    insert(conn1, table, source)
    insert(conn2, table, dest)

    assert_equal source, conn1.exec("SELECT * FROM #{table} ORDER BY 1, 2").to_a
    assert_equal dest, conn2.exec("SELECT * FROM #{table} ORDER BY 1, 2").to_a

    assert_works "#{table} #{command}", config: true

    assert_equal source, conn1.exec("SELECT * FROM #{table} ORDER BY 1, 2").to_a
    assert_equal expected, conn2.exec("SELECT * FROM #{table} ORDER BY 1, 2").to_a
  end
end
