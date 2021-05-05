class TestPlugin:
    def test_login(self, mocker) -> None:
        msg = mocker.get_one_reply("/s3_login 53")
        assert "error" in msg.text

    def test_logout(self, mocker) -> None:
        msg = mocker.get_one_reply("/s3_logout")
        assert "No estás registrado" in msg.text

    def test_get(self, mocker) -> None:
        msg = mocker.get_one_reply("/s3_get https://fsf.org")
        assert "No estás registrado" in msg.text
