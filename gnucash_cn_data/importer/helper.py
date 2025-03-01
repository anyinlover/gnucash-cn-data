from typing import Mapping, Tuple


def create_account_maps(account_map: Mapping) -> Tuple[Mapping]:
    """Divide and create reversed mapping from origin account map config"""
    la_to_accounts, lai_from_accounts, lae_from_accounts = {}, {}, {}

    for category, mapping in account_map.items():
        if category in ["Liabilities", "Assets"]:
            for key, patterns in mapping.items():
                for to_pattern in patterns[0]:
                    la_to_accounts[to_pattern] = f"{category}:{key}"
                if patterns[1]:
                    lai_from_accounts["|".join(patterns[1])] = f"{category}:{key}"
                    lae_from_accounts["|".join(patterns[1])] = f"{category}:{key}"
        elif category == "Income":
            for key, patterns in mapping.items():
                lai_from_accounts["|".join(patterns)] = f"{category}:{key}"
        elif category == "Expenses":
            for key, patterns in mapping.items():
                lae_from_accounts["|".join(patterns)] = f"{category}:{key}"
        else:
            raise ValueError(f"{category} not exist")

    return la_to_accounts, lai_from_accounts, lae_from_accounts


def create_filter_map(filters_map: Mapping, kind: str) -> Mapping:
    """Create specific filter map from origin filter map config"""
    if kind not in filters_map:
        return {}
    filter_map = filters_map[kind]
    for k, v in filter_map.items():
        filter_map[k] = "|".join(v)
    return filter_map


def create_class_map_and_import(parent_class):
    class_map = {}

    def get_subclasses(cls):
        for subclass in cls.__subclasses__():
            class_map[subclass.__name__.lower()] = subclass
            globals()[subclass.__name__] = subclass
            get_subclasses(subclass)

    get_subclasses(parent_class)
    return class_map
