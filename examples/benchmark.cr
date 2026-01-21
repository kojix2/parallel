require "../src/parallel"

# Settings
WORK_SCALE =       8
ITERATIONS = 1000000
BAR_WIDTH  =      40

default_workers = Fiber::ExecutionContext.default_workers_count
max_workers = default_workers
items = (1..(max_workers * WORK_SCALE)).to_a

def heavy_work(x : Int32)
  sum = 0.0
  ITERATIONS.times do |i|
    sum += Math.sqrt(x + i)
  end
  sum
end

puts "workers: #{default_workers}, items: #{items.size}"

seq_start = Time.instant
seq_result = items.map { |x| heavy_work(x) }
seq_time = Time.instant - seq_start

puts "sequential: #{("%.2fms" % seq_time.total_milliseconds)}"

results = [] of Tuple(Int32, Time::Span, Bool)
(1..max_workers).each do |workers|
  context = Fiber::ExecutionContext::Parallel.new("benchmark-workers-#{workers}", workers)
  par_start = Time.instant
  par_result = items.par_map(context) { |x| heavy_work(x) }
  par_time = Time.instant - par_start
  results << {workers, par_time, seq_result.map(&.round(6)) == par_result.map(&.round(6))}
end

def bar(label : String, value_ms : Float64, max_ms : Float64, width : Int32 = BAR_WIDTH)
  ratio = max_ms == 0 ? 0.0 : value_ms / max_ms
  count = (ratio * width).round.to_i
  count = 1 if count == 0 && value_ms > 0
  graph = "|" * count
  value_text = ("%.2fms" % value_ms).rjust(10)
  "#{label} #{value_text} #{graph}"
end

puts
puts "parallel (lower is better):"
max_ms = results.map { |(_, t, _)| t.total_milliseconds }.max
results.each do |workers, time, ok|
  unless ok
    raise "Benchmark results mismatch for W#{workers}"
  end

  label = "W#{workers}"
  line = bar(label, time.total_milliseconds, max_ms)
  puts line
end
