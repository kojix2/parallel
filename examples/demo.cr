require "../src/parallel"

puts "=== Crystal Parallel Processing Library Demo ==="
puts

# Basic par_map example
puts "1. Basic par_map with Array:"
result = [1, 2, 3, 4, 5].par_map { |x| x * x }
puts "Input: [1, 2, 3, 4, 5]"
puts "par_map { |x| x * x } => #{result}"
puts

# par_map with Range
puts "2. par_map with Range:"
result = (1..10).par_map { |x| x * 2 }
puts "Input: (1..10)"
puts "par_map { |x| x * 2 } => #{result}"
puts

# par_sum example
puts "3. par_sum example:"
sum = (1..100).par_sum { |x| x }
puts "Input: (1..100)"
puts "par_sum { |x| x } => #{sum}"
puts "Expected: #{(1..100).sum} ✓"
puts

# par_each example with side effects
puts "4. par_each with side effects:"
results = [] of String
mutex = Mutex.new

["apple", "banana", "cherry"].par_each do |fruit|
  processed = "processed_#{fruit}"
  mutex.synchronize do
    results << processed
  end
end

puts "Input: [\"apple\", \"banana\", \"cherry\"]"
puts "par_each with processing => #{results.sort}"
puts

# Performance comparison
puts "5. Performance comparison (I/O simulation):"
puts "Sequential processing:"
start_time = Time.monotonic
sequential_result = [1, 2, 3, 4].map do |x|
  sleep(0.1.seconds) # Simulate I/O
  x * 2
end
sequential_time = Time.monotonic - start_time
puts "Result: #{sequential_result}"
puts "Time: #{sequential_time.total_milliseconds.round(2)}ms"

puts "\nParallel processing:"
start_time = Time.monotonic
parallel_result = [1, 2, 3, 4].par_map do |x|
  sleep(0.1.seconds) # Simulate I/O
  x * 2
end
parallel_time = Time.monotonic - start_time
puts "Result: #{parallel_result}"
puts "Time: #{parallel_time.total_milliseconds.round(2)}ms"

speedup = sequential_time / parallel_time
puts "Speedup: #{speedup.round(2)}x"
puts

# Exception handling
puts "6. Exception handling:"
begin
  [1, 2, 3, 4].par_map do |x|
    if x == 3
      raise "Error at #{x}!"
    end
    x * 2
  end
rescue ex
  puts "Caught exception: #{ex.message}"
end
puts

# Custom ExecutionContext
puts "7. Custom ExecutionContext:"
custom_context = Fiber::ExecutionContext::MultiThreaded.new("demo-workers", 2)
result = [1, 2, 3, 4].par_map(custom_context) { |x| x + 10 }
puts "Using custom context with 2 workers:"
puts "Input: [1, 2, 3, 4]"
puts "par_map { |x| x + 10 } => #{result}"
puts

# Large dataset test
puts "8. Large dataset test:"
large_array = (1..1000).to_a
start_time = Time.monotonic
result = large_array.par_map { |x| Math.sqrt(x).round(2) }
elapsed = Time.monotonic - start_time

puts "Processed #{large_array.size} elements"
puts "First 10 results: #{result[0..9]}"
puts "Time: #{elapsed.total_milliseconds.round(2)}ms"
puts

puts "=== Demo completed successfully! ==="
