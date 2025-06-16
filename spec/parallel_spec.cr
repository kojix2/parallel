require "./spec_helper"

describe Parallel do
  describe "VERSION" do
    it "has a version number" do
      Parallel::VERSION.should eq("0.1.0")
    end
  end

  describe "par_map" do
    it "works with Array" do
      puts "Starting Array test"
      result = [1, 2, 3, 4].par_map { |x| x * 2 }
      puts "Array test result: #{result}"
      result.should eq([2, 4, 6, 8])
      puts "Array test completed"
    end

    it "works with Range" do
      result = (1..4).par_map { |x| x * 2 }
      result.should eq([2, 4, 6, 8])
    end

    it "works with Set" do
      result = Set{1, 2, 3, 4}.par_map { |x| x * 2 }
      result.sort.should eq([2, 4, 6, 8])
    end

    it "preserves order for Array (Indexable)" do
      result = (1..100).to_a.par_map { |x| x }
      result.should eq((1..100).to_a)
    end

    it "handles empty collections" do
      result = ([] of Int32).par_map { |x| x * 2 }
      result.should eq([] of Int32)
    end

    it "works with different return types" do
      result = [1, 2, 3].par_map { |x| x.to_s }
      result.should eq(["1", "2", "3"])
    end

    it "propagates exceptions" do
      expect_raises(Exception, "test error") do
        [1, 2, 3].par_map do |x|
          if x == 2
            raise "test error"
          end
          x * 2
        end
      end
    end

    it "works with custom ExecutionContext" do
      custom_context = Fiber::ExecutionContext::MultiThreaded.new("test", 2)
      result = [1, 2, 3, 4].par_map(custom_context) { |x| x * 2 }
      result.should eq([2, 4, 6, 8])
    end
  end

  describe "par_each" do
    it "executes block for each element" do
      results = [] of Int32
      mutex = Mutex.new

      [1, 2, 3, 4].par_each do |x|
        mutex.synchronize do
          results << x * 2
        end
      end

      results.sort.should eq([2, 4, 6, 8])
    end

    it "works with Range" do
      results = [] of Int32
      mutex = Mutex.new

      (1..4).par_each do |x|
        mutex.synchronize do
          results << x
        end
      end

      results.sort.should eq([1, 2, 3, 4])
    end

    it "handles empty collections" do
      executed = false
      ([] of Int32).par_each { |x| executed = true }
      executed.should be_false
    end

    it "propagates exceptions" do
      expect_raises(Exception, "each error") do
        [1, 2, 3].par_each do |x|
          if x == 2
            raise "each error"
          end
        end
      end
    end
  end

  describe "Indexable optimization" do
    it "uses optimized path for Array" do
      # Test that Array uses the Indexable implementation
      # This is implicit - we just verify it works correctly
      large_array = (1..1000).to_a
      result = large_array.par_map { |x| x * 2 }
      result.should eq(large_array.map { |x| x * 2 })
    end

    it "preserves order with large datasets" do
      large_range = (1..1000).to_a
      result = large_range.par_map { |x| x }
      result.should eq(large_range)
    end
  end

  describe "performance characteristics" do
    it "handles concurrent access correctly" do
      # Test with a shared counter to ensure thread safety
      counter = Atomic(Int32).new(0)

      (1..100).par_each do |x|
        counter.add(1)
      end

      counter.get.should eq(100)
    end

    it "works with I/O operations" do
      # Simulate I/O with sleep
      start_time = Time.monotonic

      [1, 2, 3, 4].par_each do |x|
        sleep(0.01.seconds) # 10ms per operation
      end

      elapsed = Time.monotonic - start_time
      # Should be faster than sequential (4 * 10ms = 40ms)
      # Allow generous margin for CI environments
      elapsed.should be < 100.milliseconds
    end
  end

  describe "chunk processing" do
    it "par_map with chunk produces same result as without chunk" do
      input = (1..20).to_a
      normal_result = input.par_map { |x| x * 2 }
      chunk_result = input.par_map(chunk: 5) { |x| x * 2 }
      
      chunk_result.should eq(normal_result)
      chunk_result.should eq(input.map { |x| x * 2 })
    end

    it "par_each with chunk produces same result as without chunk" do
      input = (1..10).to_a
      normal_results = [] of Int32
      chunk_results = [] of Int32
      mutex = Mutex.new

      input.par_each { |x| mutex.synchronize { normal_results << x } }
      input.par_each(chunk: 3) { |x| mutex.synchronize { chunk_results << x } }

      normal_results.sort.should eq(chunk_results.sort)
      normal_results.sort.should eq(input)
    end

    it "works with different chunk sizes" do
      input = (1..12).to_a
      expected = input.map { |x| x * x }

      [1, 2, 3, 4, 6, 12].each do |chunk_size|
        result = input.par_map(chunk: chunk_size) { |x| x * x }
        result.should eq(expected)
      end
    end

    it "handles chunk size larger than collection" do
      input = [1, 2, 3]
      result = input.par_map(chunk: 10) { |x| x * 2 }
      result.should eq([2, 4, 6])
    end

    it "propagates exceptions in chunk mode" do
      expect_raises(Exception, "chunk error") do
        [1, 2, 3, 4].par_map(chunk: 2) do |x|
          if x == 3
            raise "chunk error"
          end
          x * 2
        end
      end
    end

    it "works with Range in chunk mode" do
      result = (1..8).par_map(chunk: 3) { |x| x + 1 }
      result.should eq([2, 3, 4, 5, 6, 7, 8, 9])
    end

    it "preserves order in chunk mode" do
      input = (1..100).to_a
      result = input.par_map(chunk: 10) { |x| x }
      result.should eq(input)
    end
  end

  describe "adaptive chunking" do
    it "automatically determines chunk size for large collections" do
      # Large collection should use chunking automatically
      input = (1..1000).to_a
      result = input.par_map { |x| x * 2 }
      result.should eq(input.map { |x| x * 2 })
    end

    it "uses element-wise processing for small collections" do
      # Small collection should use element-wise processing
      input = [1, 2, 3, 4]
      result = input.par_map { |x| x * 2 }
      result.should eq([2, 4, 6, 8])
    end

    it "adaptive chunking produces same result as manual chunking" do
      input = (1..100).to_a
      auto_result = input.par_map { |x| x * 2 }
      manual_result = input.par_map(chunk: 25) { |x| x * 2 }
      
      auto_result.should eq(manual_result)
      auto_result.should eq(input.map { |x| x * 2 })
    end

    it "works with different collection sizes" do
      [10, 50, 100, 500, 1000].each do |size|
        input = (1..size).to_a
        result = input.par_map { |x| x + 1 }
        expected = input.map { |x| x + 1 }
        result.should eq(expected)
      end
    end

    it "adaptive chunking works with par_each" do
      input = (1..100).to_a
      results = [] of Int32
      mutex = Mutex.new

      input.par_each do |x|
        mutex.synchronize do
          results << x
        end
      end

      results.sort.should eq(input)
    end
  end

  describe "edge cases" do
    it "handles single element collections" do
      result = [42].par_map { |x| x * 2 }
      result.should eq([84])
    end

    it "handles nil values" do
      result = [1, nil, 3].par_map { |x| x }
      result.should eq([1, nil, 3])
    end

    it "works with complex objects" do
      objects = [
        {name: "Alice", age: 30},
        {name: "Bob", age: 25},
        {name: "Charlie", age: 35},
      ]

      result = objects.par_map { |obj| obj[:name] }
      result.sort.should eq(["Alice", "Bob", "Charlie"])
    end
  end
end
