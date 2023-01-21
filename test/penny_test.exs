defmodule PennyTest do
  use ExUnit.Case
  doctest Penny

  test "greets the world" do
    assert Penny.hello() == :world
  end
end
