import greenstalk

from config.parser_settings import ALL_PARSER_TASK_TYPES
from config.base_settings import QUEUE_CRAWL_TUBE, QUEUE_HOST, QUEUE_PORT


def get_beanstalkd_stats():
    client = None
    try:
        print(f"Connecting to Beanstalkd at {QUEUE_HOST}:{QUEUE_PORT}...")
        # use and watch are set to 'default' initially, specific tubes are queried directly.
        client = greenstalk.Client((QUEUE_HOST, QUEUE_PORT))
        print("Successfully connected.\n")

        print("--- All Tubes ---")
        all_tubes = client.tubes()
        print(all_tubes)
        print("\n")

        print(f"--- Stats for '{QUEUE_CRAWL_TUBE}' tube ---")
        try:
            crawl_jobs_stats = client.stats_tube(QUEUE_CRAWL_TUBE)
            for key, value in crawl_jobs_stats.items():
                print(f"  {key}: {value}")
            # Peek at reserved jobs in crawl_jobs tube
            if crawl_jobs_stats.get('current-jobs-reserved', 0) > 0:
                # print("  --- Peeking at some reserved jobs in 'crawl_jobs' ---")
                # Peeking at reserved jobs directly is not straightforward with greenstalk client.
                # Reserved jobs are held by specific connections.
                # We can see count, but not details without accessing that connection or using a lower-level interface.
                print("  NOTE: Reserved job details cannot be peeked easily with this script.")
        except greenstalk.NotFoundError:
            print(f"  Tube '{QUEUE_CRAWL_TUBE}' not found.")
        except Exception as e:
            print(f"  Error getting stats for '{QUEUE_CRAWL_TUBE}': {e}")
        print("\n")

        # Iterate through parser task types to get stats for their respective tubes
        if ALL_PARSER_TASK_TYPES:
            print("--- Stats for Parser Tubes ---")
            for task_name_key in ALL_PARSER_TASK_TYPES:
                parser_tube_name = f"crawler_htmlparser_{task_name_key}_tube"
                print(f"--- Stats for '{parser_tube_name}' tube ---")
                try:
                    # Check if tube exists first by checking against all_tubes list
                    if parser_tube_name in all_tubes:
                        parser_tube_stats = client.stats_tube(parser_tube_name)
                        for key, value in parser_tube_stats.items():
                            print(f"  {key}: {value}")
                        # Peek at reserved jobs in this parser tube
                        if parser_tube_stats.get('current-jobs-reserved', 0) > 0:
                            # print(f"  --- Peeking at some reserved jobs in '{parser_tube_name}' ---")
                            print(f"  NOTE: Reserved job details for '{parser_tube_name}' cannot be peeked easily with this script.")
                    else:
                        print(f"  Tube '{parser_tube_name}' does not exist (yet).")
                except greenstalk.NotFoundError: # Should be caught by 'in all_tubes' but good to have
                    print(f"  Tube '{parser_tube_name}' not found.")
                except Exception as e:
                    print(f"  Error getting stats for '{parser_tube_name}': {e}")
                print("") # Add a small space after each parser tube's stats
            print("\n")
        else:
            print("--- No Parser Tubes Defined in ALL_PARSER_TASK_TYPES ---")
            print("\n")

        print("--- General Beanstalkd Stats ---")
        general_stats = client.stats()
        for key, value in general_stats.items():
            print(f"  {key}: {value}")
        print("\n")

    except ConnectionRefusedError:
        print(f"Error: Connection refused. Is Beanstalkd running at {QUEUE_HOST}:{QUEUE_PORT}?")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if client:
            client.close()
            print("Connection closed.")

if __name__ == "__main__":
    get_beanstalkd_stats()