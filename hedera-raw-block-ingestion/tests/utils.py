import random
import string
from typing import List, Union


def generate_random_string(length: int, empty_probability: float = 0.0) -> str:
    """
    Generate a random string of the given length

    @param length: length of the string to generate
    @param empty_frequency: probability of generating an empty string, between 0.0 and 1.0
    @return: random string
    """

    if random.random() < empty_probability:
        return ""

    chars = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(chars) for i in range(length))


def generate_random_accounts(how_many: int = 1) -> Union[str, List[str]]:
    """
    Generate a random account ID

    @param length: how many account IDs to generate
    @return: random account ID
    """

    if how_many == 1:
        account = f"0.0.{random.randint(0, 1000000)}"
        return account
    
    accounts = [f"0.0.{random.randint(0, 1000000)}" for _ in range(how_many)]
    return accounts
    

def generate_transfer_amounts(num_accounts: int, how_many: int = 1, max_amount: float = 1e4) -> Union[List[float], List[List[float]]]:
    """
    Generate a list of random transfer amounts that involves multiple accounts with all amounts netting to zero.
    Only one account will have a negative amount, and the rest will have positive amounts.

    @param num_accounts: number of accounts involved in the transfer action
    @param how_many: how many groups of transfer amounts to generate
    @return: list of a group or multiple groups of of random transfer values
    """

    if how_many == 1:
        pay = random.random() * max_amount
        receives = [random.uniform(0, pay) for _ in range(num_accounts - 1)]
        scaling_factor = pay / sum(receives)
        receives = [receives_values * scaling_factor for receives_values in receives]

        transfers = [-pay] + receives
        return transfers

    transfer_groups = []

    for _ in range(how_many):
        pay = random.random() * max_amount
        receives = [random.uniform(0, pay) for _ in range(num_accounts - 1)]
        scaling_factor = pay / sum(receives)
        receives = [receives_values * scaling_factor for receives_values in receives]

        transfers = [-pay] + receives
        transfer_groups.append(transfers)

    return transfer_groups
