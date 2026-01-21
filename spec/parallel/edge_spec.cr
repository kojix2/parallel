require "../spec_helper"

describe "Parallel edge cases" do
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
