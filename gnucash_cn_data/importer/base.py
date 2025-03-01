from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
from piecash import open_book, factories
from .helper import create_account_maps, create_filter_map


class Base(ABC):
    def __init__(self, csv_path, accounts_map, filters_map, config_map):
        self.csv_path = csv_path
        self.filter_map = create_filter_map(filters_map, self.__class__.__name__.lower())
        self.la_account_map, self.lai_account_map, self.lae_account_map = create_account_maps(accounts_map)
        confs = config_map["postgres"]
        uri_conn = f"postgresql://{confs['user']}:{confs['password']}@{confs['ip']}:{confs['port']}/{confs['database']}"
        self.book = open_book(uri_conn=uri_conn, do_backup=False, readonly=False)

    @abstractmethod
    def read_csv(self):
        pass

    def filter_df(self):
        for k, v in self.filter_map.items():
            self.df = self.df[~self.df[k].str.contains(v, regex=True)]

    def fix_method(self):
        pass

    def fix_noflow(self):
        pass

    @abstractmethod
    def create_format_df(self):
        pass

    @abstractmethod
    def map_transfer_account(self):
        pass

    def map_to_account(self):
        def map_func(method: str):
            if method in self.la_account_map:
                return self.la_account_map[method]
            else:
                raise ValueError(f"{method}找不到对应的账户")

        self.df["account"] = self.df["to"].map(map_func)

    def clearup(self):
        pass

    def format_transactions(self):
        self.filter_df()
        self.fix_method()
        self.fix_noflow()
        self.create_format_df()
        self.map_transfer_account()
        self.map_to_account()
        self.clearup()

    def store_transactions(self):
        for row in self.df.itertuples(index=False):
            factories.single_transaction(
                post_date=row.post_date,
                enter_date=datetime.now(),
                description=row.description,
                value=Decimal(str(row.amount)),
                from_account=self.book.accounts(fullname=row.transfer),
                to_account=self.book.accounts(fullname=row.account),
            )
            self.book.flush()
        self.book.save()

    def __call__(self):
        self.read_csv()
        self.format_transactions()
        self.store_transactions()
