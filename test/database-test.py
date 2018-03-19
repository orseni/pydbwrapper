import inspect
from database import Database


def test_database():
    with Database() as db:
        find_all_users(db)
        find_user_by_id(db)
        update_user(db)
        find_all_users(db)
        find_user_by_id_named_query(db)


def find_all_users(db):
    print("test: {}".format(inspect.stack()[0][3]))
    users = db.execute("select id, name from users")
    for user in users:
        print("{} - {}".format(user.id, user.name))


def find_user_by_id(db):
    print("test: {}".format(inspect.stack()[0][3]))
    user_two = db.execute("select id, name from users where id = %(id)s", {"id":2}).fetchone()
    print(user_two)


def update_user(db):
    print("test: {}".format(inspect.stack()[0][3]))
    db.update("users").set("name", "Usuario 3").where("id", 3).execute()


def find_user_by_id_named_query(db):
    print("test: {}".format(inspect.stack()[0][3]))
    user_three = db.execute("find-user-by-id", {"id":3}).fetchone()
    print(user_three)


if __name__ == "__main__":
    test_database()    