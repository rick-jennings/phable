from phable.client import Client

# define these settings specific to your use case
uri = "http://localhost:8080/api/demo"
username = "su"
password = "su"


with Client(uri, username, password) as ph:
    his_df = ph.eval(
        """read(power and point and equipRef->siteMeter)
                        .hisRead(lastMonth)"""
    ).to_pandas()

print("Here is the Pandas DataFrame showing point history data:\n")
print(his_df)
print()
print(f"Here are the DataFrame's attributes:\n{his_df.attrs}")
