import psycopg2

def connect_RM2013():
    #Connect to an existing database
    conn = psycopg2.connect(user="postgres", password="superuser", host="localhost", port="5432", database="EcoTripRM_ga")
    return(conn)
def connect_RM2015():
    #Connect to an existing database
    conn = psycopg2.connect(user="postgres", password="superuser", host="localhost", port="5432", database="EcoTripRM_2015")
    return(conn)
def connect_viasat():
    #Connect to an existing database
    conn = psycopg2.connect(user="postgres", password="vaxcrio1", host="localhost", port="5432", database="fede_viasat")
    return(conn)
def connect_EcoTripRM():
    #Connect to an existing database
    conn = psycopg2.connect(user="postgres", password="vaxcrio1", host="localhost", port="5432", database="EcoTripRM_ga")
    return(conn)
def connect_octo2015():
    #Connect to an existing database
    conn = psycopg2.connect(user="postgres", password="vaxcrio1", host="localhost", port="5432", database="Octo2015")
    return(conn)
def connect_HAIG_Viasat_CT():
    #Connect to an existing database
    conn = psycopg2.connect(user="postgres", password="vaxcrio1", host="localhost", port="5432", database="HAIG_Viasat_CT")
    return(conn)