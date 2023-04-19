import datetime
import time

from google.cloud import bigtable

PROJECT_ID = "hallowed-trail-384200"

def write_single(project_id, instance_id, table_id):
    c = bigtable.Client(project=project_id, admin=True)
    instance = c.instance(instance_id)
    table = instance.table(table_id)

    fid_1, fid_2, fid_3 = "Cost", "Inventory", "Sales"

    for _ in range(5):
        row_key = "milk"
        row = table.direct_row(row_key)
        row.set_cell(fid_1, "seller", "Goya")
        row.set_cell(fid_1, "amount", "5000")
        row.set_cell(fid_1, "month", "March")

        row.set_cell(fid_2, "Joseph", "40")
        row.set_cell(fid_2, "James", "30")
        
        row.set_cell(fid_3, "buyer", "Walmart")
        row.set_cell(fid_3, "amount", "8000")
        row.set_cell(fid_3, "month", "April")

        response = row.commit()
        print(response)
        print("Response status: ", response)

        time.sleep(0.2)

def read_single(project_id, instance_id, table_id):
    c = bigtable.Client(project=project_id, admin=True)
    instance = c.instance(instance_id)
    table = instance.table(table_id)

    row_key = "milk"
    try:
        row = table.read_row(row_key)
        print_row(row)
    except Exception as e:
        print(e)


def print_row(row):
    print("Reading data for {}:".format(row.row_key.decode("utf-8")))
    for cf, cols in sorted(row.cells.items()):
        print("Column Family {}".format(cf))
        for col, cells in sorted(cols.items()):
            for cell in cells:
                labels = (
                    " [{}]".format(",".join(cell.labels)) if len(cell.labels) else ""
                )
                print(
                    "\t{}: {} | {}{}".format(
                        col.decode("utf-8"),
                        cell.value.decode("utf-8"),
                        cell.timestamp,
                        labels,
                    )
                )
    print("____________________________________________________________________")


if __name__ == "__main__":
    instance_id="project845"
    table_id="Groceries"
    write_single(project_id=PROJECT_ID,
                 instance_id=instance_id,
                 table_id=table_id)
    
    read_single(project_id=PROJECT_ID,
                instance_id=instance_id,
                table_id=table_id)
    