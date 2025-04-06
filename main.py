import json
from datetime import datetime
import multiprocessing
from functools import partial
import random
from collections import defaultdict
import time

NR_PUBLICATIONS = 100_000
NR_SUBSCRIPTIONS = 100_000
CONFIG_FILE = "config.json"
CITIES_FILE = "cities.txt"
DATA_TYPES = {
    "Integer": int,
    "String": str,
    "Date": datetime
}
# operators = []

FIELDS = 0
LIMITS = 1
OPERATORS = 2
MULTI_PROCESSES = 3


# Generate configuration rules from config.json
def read_rules(config):
    rules = {}
    limit_rules = {}
    fields = config[FIELDS]
    limits = config[LIMITS]

    for key in limits.keys():
        condition = limits[key]
        limit_rules[key] = []
        splited_condition = condition.split(" ")
        first_cond = splited_condition[0]
        second_cond = splited_condition[1]
        if first_cond.startswith("["):
            limit_rules[key].append(int(first_cond[1:]))
        elif first_cond.startswith("("):
            limit_rules[key].append(int(first_cond[1:]) + 1)
        else:
            print("Error in first_condition for {}!".format(first_cond))
            exit(1)

        if second_cond.endswith("]"):
            limit_rules[key].append(int(second_cond[:-1]))
        elif second_cond.endswith(")"):
            limit_rules[key].append(int(second_cond[:-1]) - 1)
        else:
            print("Error in second_condition for {}!".format(second_cond))
            exit(1)

    for field in fields:
        rules[field] = {}
        if field in limit_rules.keys():
            rules[field]["limits"] = limit_rules[field]
        if fields[field][0] in DATA_TYPES.keys():
            rules[field]["type"] = DATA_TYPES[fields[field][0]]

        if len(fields[field]) == 2:
            rules[field]["freq_field"] = fields[field][1]

        if len(fields[field]) == 3:
            rules[field]["freq_field"] = fields[field][1]
            rules[field]["freq_op"] = fields[field][2]

    return rules


def generate_publications(rules, cities, nr_publications):
    publications = []
    stats = {
        "total_pubs": nr_publications,
        "frequencies": {},
        "datetime": []
    }
    for i in range(nr_publications):
        current_publication = {}
        for key in rules.keys():
            if (rules[key]["type"] == int or rules[key]["type"] == float) and rules[key]["limits"]:
                current_publication[key] = random.randint(rules[key]["limits"][0], rules[key]["limits"][1])
                # Next block of conditions is for stats
                if key not in stats["frequencies"].keys():
                    stats["frequencies"][key] = {}
                if current_publication[key] not in stats["frequencies"][key].keys():
                    stats["frequencies"][key][current_publication[key]] = 0
                stats["frequencies"][key][current_publication[key]] += 1
                # End of stats block

            if rules[key]["type"] == str and key == "city":
                current_publication[key] = random.choice(cities)
                # Next block of conditions is for stats
                if key not in stats["frequencies"].keys():
                    stats["frequencies"][key] = {}
                if current_publication[key] not in stats["frequencies"][key].keys():
                    stats["frequencies"][key][current_publication[key]] = 0
                stats["frequencies"][key][current_publication[key]] += 1
                # End of stats block

            if rules[key]["type"] == datetime:
                current_publication[key] = str(datetime.now())
                stats["datetime"].append(current_publication[key])

        publications.append(current_publication)

    return publications, stats


def generate_publications_multiprocess(rules, cities, num_processes, publication_chunks):
    with multiprocessing.Pool(processes=num_processes) as pool:
        results = pool.map(partial(generate_publications, rules, cities), publication_chunks)

    # Combine results
    all_publications = []
    all_stats = []
    for pubs, stats in results:
        all_publications.extend(pubs)
        all_stats.append(stats)

    merged_stats = {
        "total_pubs": 0,
        "frequencies": defaultdict(lambda: defaultdict(int)),  # <-- nested defaultdict
        "datetime": []
    }

    for stat in all_stats:
        merged_stats["total_pubs"] += stat.get("total_pubs", 0)
        for key, subdict in stat.get("frequencies", {}).items():
            if key not in merged_stats["frequencies"]:
                merged_stats["frequencies"][key] = defaultdict(int)
            for subkey, count in subdict.items():
                merged_stats["frequencies"][key][subkey] += count
        merged_stats["datetime"].extend(stat.get("datetime", []))

    merged_stats["frequencies"] = {
        key: dict(subdict)
        for key, subdict in merged_stats["frequencies"].items()
    }
    return all_publications, merged_stats


# Check the fields that must be generated in the subscription
def get_field_list_from_sub_gen(current_index, field_nr_gen):
    lst = []
    for item in field_nr_gen.keys():
        if field_nr_gen[item] > current_index:
            lst.append(item)
    return lst


# Generate values for current field
def get_subscription_values(field, stats, rules, operators):
    sub_value = ()
    if field == "city":
        max_val_city = 0
        max_val_city_name = ""
        for city, freq in stats["frequencies"]["city"].items():
            if max_val_city < freq:
                max_val_city = freq
                max_val_city_name = city
        stats["frequencies"]["city"][max_val_city_name] -= 1
        sub_value = (field, "=", max_val_city_name)
    elif rules[field]["type"] == int:
        field_value = random.randint(rules[field]["limits"][0], rules[field]["limits"][1])
        field_op = random.choice(operators)
        sub_value = (field, field_op, field_value)
    elif rules[field]["type"] == datetime:
        field_value = random.choice(stats["datetime"])
        field_op = random.choice(operators)
        sub_value = (field, field_op, str(field_value))
    return sub_value


def generate_subscriptions(rules, stats, cities, operators, nr_subs):
    subscriptions = []
    field_nr_gen = {}
    op_nr_gen = {}

    for rule in rules.keys():
        if rules[rule]["freq_field"]:
            # Calculate number of subscriptions for each field by freq_field
            field_nr_gen[rule] = rules[rule]["freq_field"] * nr_subs // 100
        if "freq_op" in rules[rule].keys() and rules[rule]["freq_op"]:
            # Calculate number of subscriptions for  '=' operator by freq_op
            op_nr_gen[rule] = rules[rule]["freq_op"] * nr_subs // 100

    max_freq_field = max(field_nr_gen.values())
    max_freq_op = max(op_nr_gen.values())
    if max_freq_field < max_freq_op:
        print("The number of frequency operators for city is lower than the number of cities to generate!")
        exit(1)
    idx = 0

    while idx < nr_subs:
        current_sub = []
        # Get mandatory fields for current subscription and number of them
        lst = get_field_list_from_sub_gen(idx, field_nr_gen)

        # Generate number of fields for current subscription between
        # max(1,number_of_mandatory_fields) and number_of_fields
        nr_fields_for_sub = random.randint(max(1, len(lst)), len(rules.keys()))

        for field in lst:
            current_sub.append(get_subscription_values(field, stats, rules, operators))

        # If number_of_fields is greater than number_of_mandatory_fields, add fields from remaining fields
        if nr_fields_for_sub > len(lst):
            remaining = nr_fields_for_sub - len(lst)
            remaining_fields = [item for item in rules.keys() if item not in lst]
            fields_to_add = random.sample(remaining_fields, remaining)
            for field in fields_to_add:
                current_sub.append(get_subscription_values(field, stats, rules, operators))

        subscriptions.append(current_sub)
        idx += 1

    return subscriptions


def generate_subscriptions_multiprocess(rules, stats, cities, num_processes, sub_chunks, operators):
    with multiprocessing.Pool(processes=num_processes) as pool:
        results = pool.map(partial(generate_subscriptions, rules, stats, cities, operators), sub_chunks)

    # Combine results
    all_subscription = []
    for sub in results:
        all_subscription.extend(sub)

    return all_subscription


def main():
    # global operators
    cities = []

    with open(CONFIG_FILE, "r") as ff:
        config_data = json.load(ff)
    if not config_data:
        print("Error when loading the config file!!")
        exit(1)

    with open(CITIES_FILE, "r") as ff:
        for line in ff:
            cities.append(line.strip())
    if len(cities) == 0:
        print("Error when loading the cities!!")
        exit(1)

    rules = read_rules(config_data)
    operators = config_data[OPERATORS]
    is_multi_proc = config_data[MULTI_PROCESSES]["IS_MULTI_PROC"]
    num_processes = config_data[MULTI_PROCESSES]["NUMBER_OF_PROC"]

    if is_multi_proc:
        chunk_size = NR_PUBLICATIONS // num_processes
        remainder = NR_PUBLICATIONS % num_processes
        publication_chunks = [
            chunk_size + (1 if i < remainder else 0)
            for i in range(num_processes)
        ]

        publications, stats = generate_publications_multiprocess(rules, cities, num_processes, publication_chunks)
    else:
        publications, stats = generate_publications(rules, cities, NR_PUBLICATIONS)

    if len(publications) == 0:
        print("No publications are generating !!")
        exit(1)
    else:
        with open("publications.txt", "w", encoding="utf-8") as f:
            for pub in publications:
                f.write(json.dumps(pub, ensure_ascii=False) + "\n")

    if is_multi_proc:
        chunk_size = NR_SUBSCRIPTIONS // num_processes
        remainder = NR_SUBSCRIPTIONS % num_processes
        sub_chunks = [
            chunk_size + (1 if i < remainder else 0)
            for i in range(num_processes)
        ]
        subscriptions = generate_subscriptions_multiprocess(rules, stats, cities, num_processes, sub_chunks, operators)
    else:
        subscriptions = generate_subscriptions(rules, stats, cities, operators, NR_SUBSCRIPTIONS)

    if len(subscriptions) == 0:
        print("No subscriptions are generating !!")
        exit(1)
    else:
        with open("subscriptions.txt", "w", encoding="utf-8") as f:
            for sub in subscriptions:
                f.write(json.dumps(sub, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Execution time: {execution_time:.4f} seconds")
