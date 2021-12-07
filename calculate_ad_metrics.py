import sys

fp = open("sim_raw_input_data.tsv")
contents = fp.read().splitlines()
contents.pop(0)

unique_list = []
    
# traverse for all elements
for x in contents:
    # check if exists in unique_list or not
    if x not in unique_list:
        unique_list.append(x)

parsed_metrics = []
for metric in unique_list:
    parsed_metrics.append(metric.split("\t"))

impressions_by_day = {}
for metric in parsed_metrics:
    if (metric[0] + "\t" + metric[1].split(" ")[0]) in impressions_by_day:
        impressions_by_day[metric[0] + "\t" + metric[1].split(" ")[0]].add(metric[3])
    else:
        impressions_by_day[metric[0] + "\t" + metric[1].split(" ")[0]] = set()
        impressions_by_day[metric[0] + "\t" + metric[1].split(" ")[0]].add(metric[3])


print("Total impressions by day:")
print("ad_id\tday\tnumber_of_impressions")
for ad in impressions_by_day:
    print(ad + "\t" + str(len(impressions_by_day[ad])))

impressions_by_session = {}
for metric in parsed_metrics:
    if (metric[0] + "\t" + metric[1].split(" ")[0] + "\t" + metric[5]) in impressions_by_session:
        impressions_by_session[metric[0] + "\t" + metric[1].split(" ")[0] + "\t" + metric[5]].add(metric[3])
    else:
        impressions_by_session[metric[0] + "\t" + metric[1].split(" ")[0] + "\t" + metric[5]] = set()
        impressions_by_session[metric[0] + "\t" + metric[1].split(" ")[0] + "\t" + metric[5]].add(metric[3])


print("Total impressions by session:")
print("ad_id\tday\tip_address\tnumber_of_impressions")
for ad in impressions_by_session:
    print(ad + "\t" + str(len(impressions_by_session[ad])))

impressions_by_user = {}
for metric in parsed_metrics:
    if (metric[0] + "\t" + metric[4]) in impressions_by_user:
        impressions_by_user[metric[0] + "\t" + metric[4]].add(metric[3])
    else:
        impressions_by_user[metric[0] + "\t" + metric[4]] = set()
        impressions_by_user[metric[0] + "\t" + metric[4]].add(metric[3])


print("Total impressions by user:")
print("ad_id\tuser_id\tnumber_of_impressions")
for ad in impressions_by_user:
    print(ad + "\t" + str(len(impressions_by_user[ad])))

conversion_per_impression = {}
for metric in parsed_metrics:
    if metric[0] in conversion_per_impression:
        if metric[2] == "click":
            conversion_per_impression[metric[0]]["clicks"] = conversion_per_impression[metric[0]]["clicks"] + 1
        conversion_per_impression[metric[0]]["total"] = conversion_per_impression[metric[0]]["total"] + 1
    else:
        conversion_per_impression[metric[0]] = {"clicks": 0, "total": 0}
        if metric[2] == "click":
            conversion_per_impression[metric[0]]["clicks"] = conversion_per_impression[metric[0]]["clicks"] + 1
        conversion_per_impression[metric[0]]["total"] = conversion_per_impression[metric[0]]["total"] + 1


print("Conversion rates by impression:")
print("ad_id\tconversion_rate")
for ad in conversion_per_impression:
    print(ad + "\t" + str(conversion_per_impression[ad]["clicks"] / conversion_per_impression[ad]["total"]))

conversion_per_session = {}
for metric in parsed_metrics:
    if metric[0] in conversion_per_session:
        if metric[2] == "click":
            conversion_per_session[metric[0]]["clicks"] = conversion_per_session[metric[0]]["clicks"] + 1
        conversion_per_session[metric[0]]["sessions"].add(metric[1].split(" ")[0] + "\t" + metric[5])
    else:
        conversion_per_session[metric[0]] = {"clicks": 0, "sessions": set()}
        if metric[2] == "click":
            conversion_per_session[metric[0]]["clicks"] = conversion_per_session[metric[0]]["clicks"] + 1
        conversion_per_session[metric[0]]["sessions"].add(metric[1].split(" ")[0] + "\t" + metric[5])


print("Conversion rates by session:")
print("ad_id\tconversion_rate")
for ad in conversion_per_session:
    print(ad + "\t" + str(conversion_per_session[ad]["clicks"] / len(conversion_per_session[ad]["sessions"])))

conversion_per_user = {}
for metric in parsed_metrics:
    if metric[0] in conversion_per_user:
        if metric[2] == "click":
            conversion_per_user[metric[0]]["clicks"] = conversion_per_user[metric[0]]["clicks"] + 1
        conversion_per_user[metric[0]]["users"].add(metric[4])
    else:
        conversion_per_user[metric[0]] = {"clicks": 0, "users": set()}
        if metric[2] == "click":
            conversion_per_user[metric[0]]["clicks"] = conversion_per_user[metric[0]]["clicks"] + 1
        conversion_per_user[metric[0]]["users"].add(metric[4])


print("Conversion rates by user:")
print("ad_id\tconversion_rate")
for ad in conversion_per_user:
    print(ad + "\t" + str(conversion_per_user[ad]["clicks"] / len(conversion_per_user[ad]["users"])))