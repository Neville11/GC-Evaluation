import datetime

from google.cloud import bigtable

def write_single(project_id, instance_id, table_id):
    c = bigtable.Client(project=project_id, admin=True)
    instance = c.instance(instance_id)
    table = instance.table(table_id)

    fid_1, fid_2, fid_3 = "cost", "inventory", "sales"
    row_key = "milk"
    row = table.direct_row(row_key)
    row.set_cell(fid_1, "seller", "Goya")
    row.set_cell(fid_1, "amount", 5000)
    row.set_cell(fid_1, "month", "March")

    row.set_cell(fid_2, "Joseph", 40)
    row.set_cell(fid_2, "James", 30)
    
    row.set_cell(fid_3, "buyer", "Walmart")
    row.set_cell(fid_3, "amount", 8000)
    row.set_cell(fid_3, "month", "April")

    response = row.commit()
    print("Response status: ", response)


if __name__ == "__main__":
    write_single(project_id="project845",
                 instance_id="project845",
                 table_id="Groceries")
    