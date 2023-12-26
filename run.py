from scripts.utils import clean_url_list, update_articles_table, create_postgres_connection, create_postges_table_if_not_exist, check_postgres_connection, create_scraped_url_list, main_scrape

# Test DB connection
is_successful  = check_postgres_connection()
if is_successful:
    # Scape articles url list
    max_page = 5
    url_list = create_scraped_url_list(max_page)

    # Connect to DB
    conn = create_postgres_connection()
    cursor = conn.cursor()

    # Check if url exists and remove
    url_clean_list = clean_url_list(url_list, conn)

    # Scrape main info for each url article
    main_list = main_scrape(url_clean_list)

    # Create Articles Table if not exist
    create_postges_table_if_not_exist(conn, cursor)

    # Update Postgres articles table
    update_articles_table(conn, cursor, main_list)

    # Stop connection
    cursor.close()
    conn.close()    
else:
    print("Not Successful Connection Test.")