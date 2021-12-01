import time
import uuid
from random import randint

import faker
import sqlalchemy
from mimesis import Person, Address, Datetime
from sqlalchemy import Integer, Column, Date, FLOAT, String, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class People(Base):
    __tablename__ = 'people'
    id_code = Column(Integer, unique=True, primary_key=True, nullable=False)
    customer_id = Column(String(100), unique=True, nullable=False)
    name = Column(String(32), nullable=False)
    second_name = Column(String(32))
    surname = Column(String(32), nullable=False)
    phone = Column(String(50), nullable=False)
    city = Column(String(32), nullable=False)
    birth_date = Column(Date(), nullable=False)

    def __init__(self):
        self.customer_id = str(uuid.uuid4())
        self.name = Person().name()
        self.second_name = Person().last_name()
        self.surname = Person().surname()
        self.phone = Person().telephone()
        self.city = Address().city()
        self.birth_date = Datetime().date()

    def __repr__(self):
        return f"<People({self.id_code},{self.customer_id}, {self.name},{self.second_name}, " \
               f"{self.surname},{self.phone}, {self.city}, {self.birth_date})>"


class Cards(Base):
    __tablename__ = 'cards'
    card_id = Column(String(100), unique=True, primary_key=True)
    holder_id = Column(String(100), ForeignKey(People.customer_id), nullable=False)
    card_no = Column(String(16), unique=True, nullable=False)
    valid_until = Column(String(256))
    created_on = Column(String(256))
    last_used_on = Column(String(256))
    currency = Column(String(32), nullable=False)
    amount = Column(FLOAT, nullable=False)

    def __init__(self):
        self.card_id = str(uuid.uuid4())
        self.card_no = ''.join(["{}".format(randint(0, 9)) for num in range(0, 16)])
        self.created_on = int(time.time())
        self.valid_until = int(self.created_on) + 518400
        self.last_used_on = None
        self.currency = "Ua"
        self.amount = 0

    def __repr__(self):
        return f"<Cards({self.card_id},{self.holder_id}, {self.card_no},{self.valid_until}, " \
               f"{self.created_on},{self.last_used_on}, {self.currency}, {self.amount})>"


class Transactions(Base):
    __tablename__ = 'transactions'
    transaction_id = Column(String(100), primary_key=True, unique=True, )
    card_number = Column(String(16), ForeignKey(Cards.card_no), nullable=False)
    transaction_time = Column(String(256))
    comment = Column(String(999))
    value = Column(FLOAT)

    def __init__(self):
        self.transaction_id = str(uuid.uuid4())
        self.transaction_time = int(time.time())
        self.comment = faker.Faker().text()
        self.value = randint(-10000, 10000)

    def __repr__(self):
        return f"<Transactions({self.transaction_id},{self.card_no}, {self.transaction_time},{self.comment},{self.value})>"


if __name__ == '__main__':
    import configparser

    config = configparser.ConfigParser()
    config.read('C:\\Users\\schepak\\SQLALCHEMY_Aitflow\\dags\\SQLalchemy_task\\E_B\\cred\\cred.ini')
    ENGINE = create_engine(
        f'mysql+mysqlconnector://{config.get("MySQL", "user")}:{config.get("MySQL", "password")}'
        f'@host.docker.internal:{config.get("MySQL", "port")}/{config.get("MySQL", "database")}')
    meta = sqlalchemy.MetaData()
    Base.metadata.create_all(ENGINE)
