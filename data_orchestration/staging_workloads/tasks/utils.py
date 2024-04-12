from sqlalchemy.dialects.postgresql import insert

def insert_on_conflict_nothing(table, conn, keys, data_iter):
     # "a" is the primary key in "conflict_table"
     data = [dict(zip(keys, row)) for row in data_iter]
     stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=["product_id"])
     result = conn.execute(stmt)
     return result.rowcount

def insert_on_conflict_nothing_user(table, conn, keys, data_iter):
     # "a" is the primary key in "conflict_table"
     data = [dict(zip(keys, row)) for row in data_iter]
     stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=["user_id"])
     result = conn.execute(stmt)
     return result.rowcount

def insert_on_conflict_nothing_tracking(table, conn, keys, data_iter):
     # "a" is the primary key in "conflict_table"
     data = [dict(zip(keys, row)) for row in data_iter]
     stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=["product_id", "date"])
     result = conn.execute(stmt)
     return result.rowcount

def insert_on_conflict_nothing_users_staging(table, conn, keys, data_iter):
     # "a" is the primary key in "conflict_table"
     data = [dict(zip(keys, row)) for row in data_iter]
     stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=["user_id", "date"])
     result = conn.execute(stmt)
     return result.rowcount

def insert_on_conflict_images_staging(table, conn, keys, data_iter):
     # "a" is the primary key in "conflict_table"
     data = [dict(zip(keys, row)) for row in data_iter]
     stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=["image", "product_id"])
     result = conn.execute(stmt)
     return result.rowcount

def insert_on_conflict_nothing_brands(table, conn, keys, data_iter):
     # "a" is the primary key in "conflict_table"
     data = [dict(zip(keys, row)) for row in data_iter]
     stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=["brand_id", "date"])
     result = conn.execute(stmt)
     return result.rowcount

def upsert_brands_dim(table, conn, keys, data_iter):
     # "a" is the primary key in "conflict_table"
     data = [dict(zip(keys, row)) for row in data_iter]
     stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=["brand_id"])
     result = conn.execute(stmt)
     return result.rowcount

def upsert_catalog_staging(table, conn, keys, data_iter):
     # "a" is the primary key in "conflict_table"
     data = [dict(zip(keys, row)) for row in data_iter]
     stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=["catalog_id", "date"])
     result = conn.execute(stmt)
     return result.rowcount




  