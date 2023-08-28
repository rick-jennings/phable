from zoneinfo import available_timezones

iana_timezones = available_timezones()

# read the haystack tz file
# Source:  https://project-haystack.org/download/tz.txt
with open("haystack_tz.txt", "r") as file:
    haystack_timezones = [line.replace("\n", "") for line in file]


# create a map from haystack to iana timezones
tz_map = []
for haystack_tz in haystack_timezones:
    for iana_tz in iana_timezones:
        if haystack_tz in iana_tz:
            pair = (haystack_tz, iana_tz)
            break
    tz_map.append(pair)


# write the haystack to iana timezone map
with open("tz_map.txt", "w") as file:
    file.write("haystack,iana\n")
    for pair in tz_map:
        file.write(pair[0] + "," + pair[1] + "\n")
