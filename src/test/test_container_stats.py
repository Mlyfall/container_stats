import pytest
import src.container_stats as script


def test_stats():
    assert

def test_traffic_calculation():
    assert script.calc_traffic("not_there.csv") == "Stats file not accessible."

    with pytest.raises(TypeError):
        script.calc_traffic(10)

