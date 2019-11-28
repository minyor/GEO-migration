
migration_conf = {
    "debug": False,

    # Migration parameters
    #"old_infrastructure_path": "/media/Root_2/Data/Dev/Migration/GEO_infrastructure",
    #"new_infrastructure_path": "/media/Root_2/Data/Dev/Migration/GEO",
    "old_infrastructure_path": "/media/Root_2/Data/Dev/Migration/io",
    "new_infrastructure_path": "/media/Root_2/Data/Dev/Migration/io_new",
    "mod_network_client_path": "/media/Root_2/Data/Dev/Migration/geo_network_client_mod",
    "gns_address_separator": "#",
    "unknown_address": "user_unknown#geo.pay",

    # Comparision parameters
    "old_network_client_path": "/media/Root_2/Data/Dev/Migration/client_prod",
    "old_uuid_2_address_path": "/media/Root_2/Data/Dev/Migration/uuid2address-grn/uuid2address",
    "new_network_client_path": "/media/Root_2/Data/Dev/Migration/geo_network_client",

    # Correlation parameters
    "old_handler_url": "0.0.0.0:2001",
    "new_handler_url": "0.0.0.0:2000",
    "redis_host": "localhost",
    "redis_port": "6379",
    "redis_password": "",
    "redis_db": 0
}
