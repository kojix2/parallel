require "../spec_helper"

describe "Parallel chunking" do
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
