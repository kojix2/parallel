require "../spec_helper"

describe "Parallel adaptive chunking" do
  it "automatically determines chunk size for large collections" do
    input = (1..1000).to_a
    result = input.par_map { |x| x * 2 }
    result.should eq(input.map { |x| x * 2 })
  end

  it "uses element-wise processing for small collections" do
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
