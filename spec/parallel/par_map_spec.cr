require "../spec_helper"

describe "Parallel par_map" do
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
    result = [1, 2, 3].par_map(&.to_s)
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
    custom_context = Fiber::ExecutionContext::Parallel.new("test", 2)
    result = [1, 2, 3, 4].par_map(custom_context) { |x| x * 2 }
    result.should eq([2, 4, 6, 8])
  end

  it "does not deadlock on exceptions in Indexable par_map" do
    done = Channel(Nil).new

    spawn do
      expect_raises(Exception, "boom") do
        (1..100).to_a.par_map do |x|
          raise "boom" if x % 10 == 0
          x
        end
      end
      done.send(nil)
    end

    select
    when done.receive
      # ok
    when timeout 2.seconds
      raise "timeout waiting for par_map"
    end
  end
end
