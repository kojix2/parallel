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

  describe "par_sum" do
    it "works with Array" do
      result = [1, 2, 3, 4].par_sum { |x| x }
      result.should eq(10)
    end

    it "works with Range" do
      result = (1..10).par_sum { |x| x }
      result.should eq(55)
    end

    it "works with transformations" do
      result = [1, 2, 3, 4].par_sum { |x| x * 2 }
      result.should eq(20)
    end

    it "handles empty collections" do
      result = ([] of Int32).par_sum { |x| x }
      result.should eq(0)
    end

    it "works with floats" do
      result = [1.5, 2.5, 3.5].par_sum { |x| x }
      result.should eq(7.5)
    end

    it "propagates exceptions" do
      expect_raises(Exception, "sum error") do
        [1, 2, 3].par_sum do |x|
          if x == 2
            raise "sum error"
          end
          x
        end
      end
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
      # Allow some margin for overhead
      elapsed.should be < 30.milliseconds
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
