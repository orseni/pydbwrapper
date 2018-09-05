from pydbwrapper import database
from pydbwrapper import config

config.Config.instance("config-test.json")


def test_create_table_users():
    with database.Database() as db:
        db.execute(
            "drop table if exists users")
        db.execute(
            "create table if not exists users(id int primary key, name varchar(255), birth date, gender char)")


def test_truncate_table():
    with database.Database() as db:
        db.execute("truncate table users")


def test_insert_users():
    with database.Database() as db:
        db.insert("users") \
            .set("id", 1) \
            .set("name", "User 1") \
            .set("birth", "2018-03-20") \
            .set("gender", "M") \
            .execute()
        db.insert("users").set("id", 2) \
            .set("name", "User 2") \
            .set("birth", "2018-02-20") \
            .set("gender", "F") \
            .execute()
        db.insert("users") \
            .setall({"id": 3,  "name": "User 3", "birth": "2018-01-20"}) \
            .set("gender", "M") \
            .execute()
        db.insert("users") \
            .setall({"id": 4,  "name": "Usuario 4", "birth": "2017-01-20"}) \
            .set("gender", "M") \
            .execute()


def test_find_all_users():
    with database.Database() as db:
        users = db.execute("select id, name from users").fetchall()
        assert len(users) == 4
        assert users[0].id == 1
        assert users[1].name == "User 2"
        cursor = db.execute("select id, name from users")
        for u in cursor:
            assert u.id is not None


def test_find_two_users():
    with database.Database() as db:
        users = db.execute("select id, name from users").fetchmany(2)
        assert len(users) == 2
        assert users[0].id == 1
        assert users[1].name == "User 2"


def test_find_user_by_id():
    with database.Database() as db:
        user_two = db.execute("select id, name from users where id = %(id)s", {
                              "id": 2}).fetchone()
        assert user_two.id == 2
        assert user_two.name == "User 2"


def test_update_user():
    with database.Database() as db:
        c = db.update("users").set(
            "name", "Usuario 3").where("id", 3).execute()
        assert c.cursor.rowcount == 1


def test_find_user_by_select():
    with database.Database() as db:
        users = db.select("users").where(
            "name", "User%", "like").execute().fetchall()
        assert len(users) == 2


def test_find_user_by_select_without_where():
    with database.Database() as db:
        users = db.select("users").execute().fetchall()
        assert len(users) == 4


def test_paging_user_by_select():
    with database.Database() as db:
        page = db.select("users") \
            .fields('id', 'name') \
            .where("id", "10", "<") \
            .order_by('id') \
            .paging(0, 3)
        assert type(page) == database.Page
        assert len(page.data) == 3
        assert page.data[0].id == 1
        assert page.data[0].name is not None
        try:
            page.data[0].birth
            assert False  # falhou!
        except Exception as e:
            assert isinstance(e, AttributeError)
        assert not page.last_page
        page = db.select("users").where("id", "10", "<").paging(1, 3)
        assert type(page) == database.Page
        assert len(page.data) == 1
        assert page.last_page


def test_paging_user_by_select_without_where():
    with database.Database() as db:
        page = db.select("users").paging(1, 2)
        assert type(page) == database.Page
        assert len(page.data) == 2


def test_paging_complex_query():
    with database.Database() as db:
        page = db.paging(
            "select * from users where id < %(id)s", {'id': 10}, 0, 3)
        assert type(page) == database.Page
        assert len(page.data) == 3
        assert not page.last_page
        page = db.paging(
            "select * from users where id < %(id)s", {'id': 10}, 1, 3)
        assert type(page) == database.Page
        assert len(page.data) == 1
        assert page.last_page


def test_group_by():
    with database.Database() as db:
        result = db.select("users") \
            .fields('gender sexo', 'count(1) contagem') \
            .where('id', 10, '<') \
            .group_by('gender') \
            .order_by('contagem desc') \
            .execute() \
            .fetchall()
        assert result[0].sexo == 'M'
        assert result[0].contagem == 3
        assert result[1].sexo == 'F'
        assert result[1].contagem == 1

def test_update_user_whereall():
    with database.Database() as db:
        try:
            db.update("users").set("name", "Usuario 3").whereall(
                {"id": 1, "name": "User1"}).execute()
        except() as e:
            assert e is None


def test_find_user_by_id_named_query():
    with database.Database() as db:
        user_three = db.execute("find-user-by-id", {"id": 3}).fetchone()
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


def test_select_without_result():
    with database.Database() as db:
        u = db.select('users').where('1', '0', constant=True).execute().fetchone()
        assert u is None


def test_rollback_with():
    try:
        with database.Database() as db:
            db.insert("users") \
                .set("id", 11) \
                .set("name", "User 11") \
                .set("birth", "2018-03-20") \
                .set("gender", "M") \
                .execute()
            raise Exception()
    except:
        with database.Database() as db:
            assert db.select('users').where('id', 11).execute().fetchone() is None
        