require "../spec_helper"

describe "Parallel par_each" do
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

  it "increments shared counters safely" do
    counter = Atomic(Int32).new(0)

    (1..100).par_each do |_|
      counter.add(1)
    end

    counter.get.should eq(100)
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
    ([] of Int32).par_each { |_| executed = true }
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

  it "does not deadlock on multiple exceptions in chunked par_each" do
    assert_completes_within(5.seconds) do
      expect_raises(Exception, "each boom") do
        (1..50).to_a.par_each(chunk: 5) do |x|
          raise "each boom" if x % 7 == 0
        end
      end
    end
  end
end
