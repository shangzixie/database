

Explanation:
    For CSVDataTable, we normally use dictionary to manipulate data.

    When we are trying to find data by template, we compare each key and value in the whole datatable and return those matched.

    When we are trying to find data by primary keys, we simply convert the primary key to a template, which is a kind of dictionary. And then we use the method find_by_template.

    When we are trying to delete rows, it means to find the rows first, and then delete it.

    When we are trying to update rows, it means to check if there is a row with the same primary key. If No, raise ValueError. If Yes, upgrade the values.

    When we are trying to insert rows, it means to check if there is a row with the same primary key. If Yes, raise ValueError("duplicate keys"). If No, append a row.



    For RDBDataTable, we connect to the MySql first, and output sql to manipulate the database.

    Since the MySql and execute the output, our job is to make a correct sentence that can read by MySql.

    It is in this way:
    select * from people where nameLast = "Williams"
    delete from peolpe where nameFirst = "Ted"


Tips:
    _get_defult_connection() is outside the class.
    Don't forget to check duplicate keys.
    Check the connection before run_q.
    while testing the delete function, we can use find_by_template to check if we deleted correctly.
    MySql is more convenient.