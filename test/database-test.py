from pydbwrapper import database

db = database.Database()


def test_truncate_table():
    db.execute("truncate table users")


def test_insert_users():
    db.insert("users").set("id", 1).set("name", "User 1").set("birth", "2018-03-20").execute()
    db.insert("users").set("id", 2).set("name", "User 2").set("birth", "2018-02-20").execute()
    db.insert("users").setall({"id": 3,  "name": "User 3", "birth": "2018-01-20"}).execute()


def test_find_all_users():
    users = db.execute("select id, name from users").fetchall()
    assert len(users) == 3
    assert users[0].id == 1
    assert users[1].name == "User 2"


def test_find_two_users():
    users = db.execute("select id, name from users").fetchmany(2)
    assert len(users) == 2
    assert users[0].id == 1
    assert users[1].name == "User 2"


def test_find_user_by_id():
    user_two = db.execute("select id, name from users where id = %(id)s", {"id":2}).fetchone()
    assert user_two.id == 2
    assert user_two.name == "User 2"


def test_update_user():
    try:
        result = db.update("users").set("name", "Usuario 3").where("id", 3).execute()
    except() as e:
        assert e is None


def test_find_user_by_id_named_query():
    user_three = db.execute("find-user-by-id", {"id":3}).fetchone()
    assert user_three.id == 3
    assert user_three.name == "Usuario 3"


def test_delete_user_by_id():
    db.delete("users").where("id", 3).execute()
    assert len(db.execute("select id from users").fetchall()) == 2


def test_delete_user_like_name():
    db.delete("users").where("name", "Use%", "like").execute()
    assert len(db.execute("select id from users").fetchall()) == 0

