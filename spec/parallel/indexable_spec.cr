require "../spec_helper"

describe "Parallel Indexable" do
  it "uses optimized path for Array" do
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
