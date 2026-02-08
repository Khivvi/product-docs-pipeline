from pipeline.http import host_throttle_sleep


def test_throttle_sleeps_when_needed():
    host_next = {"example.com": 200.0}

    slept = {"n": 0}

    def fake_now():
        return 100.0

    def fake_sleep(_):
        slept["n"] += 1

    host_throttle_sleep(
        host_next, "example.com", 0.3, now_fn=fake_now, sleep_fn=fake_sleep
    )
    assert slept["n"] == 1
