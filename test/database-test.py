from pydbwrapper import database
from pydbwrapper import config


config.Config.instance("config-test.json")


def test_truncate_table():
    with database.Database() as db:
        db.execute("truncate table users")


def test_insert_users():
    with database.Database() as db:
        db.insert("users").set("id", 1).set("name", "User 1").set("birth", "2018-03-20").execute()
        db.insert("users").set("id", 2).set("name", "User 2").set("birth", "2018-02-20").execute()
        db.insert("users").setall({"id": 3,  "name": "User 3", "birth": "2018-01-20"}).execute()
        db.insert("users").setall({"id": 4,  "name": "Usuario 4", "birth": "2017-01-20"}).execute()


def test_find_all_users():
    with database.Database() as db:
        users = db.execute("select id, name from users").fetchall()
        assert len(users) == 4
        assert users[0].id == 1
        assert users[1].name == "User 2"


def test_find_two_users():
    with database.Database() as db:
        users = db.execute("select id, name from users").fetchmany(2)
        assert len(users) == 2
        assert users[0].id == 1
        assert users[1].name == "User 2"


def test_find_user_by_id():
    with database.Database() as db:
        user_two = db.execute("select id, name from users where id = %(id)s", {"id":2}).fetchone()
        assert user_two.id == 2
        assert user_two.name == "User 2"


def test_update_user():
    with database.Database() as db:
        try:
            db.update("users").set("name", "Usuario 3").where("id", 3).execute()
        except() as e:
            assert e is None


def test_find_user_by_select():
    with database.Database() as db:
        users = db.select("users").where("name", "User%", "like").execute().fetchall()
        assert len(users) == 2


def test_find_user_by_select_without_where():
    with database.Database() as db:
        users = db.select("users").execute().fetchall()
        assert len(users) == 4


def test_paging_user_by_select():
    with database.Database() as db:
        page = db.select("users").where("id", "10", "<").paging(1, 2)
        assert type(page) == database.Page
        assert len(page.data) == 2


def test_paging_user_by_select_without_where():
    with database.Database() as db:
        page = db.select("users").paging(1, 2)
        assert type(page) == database.Page
        assert len(page.data) == 2


def test_update_user_whereall():
    with database.Database() as db:
        try:
            db.update("users").set("name", "Usuario 3").whereall({"id": 1, "name": "User1"}).execute()
        except() as e:
            assert e is None


def test_find_user_by_id_named_query():
    with database.Database() as db:
        user_three = db.execute("find-user-by-id", {"id":3}).fetchone()
        assert user_three.id == 3
        assert user_three.name == "Usuario 3"


def test_delete_user_by_id():
    with database.Database() as db:
        db.delete("users").where("id", 3).execute()
        assert len(db.execute("select id from users").fetchall()) == 3


def test_delete_user_like_name():
    with database.Database() as db:
        db.delete("users").where("name", "Use%", "like").execute()
        assert len(db.execute("select id from users").fetchall()) == 1

