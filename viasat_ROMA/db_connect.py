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
def connect_EcoTripCT():
    #Connect to an existing database
    conn = psycopg2.connect(user="postgres", password="superuser", host="192.168.132.18",
                            port="5432", database="EcoTripCT")
    return(conn)

# def connect_HAIG_Viasat_CT():
#    #Connect to an existing database
#     conn = psycopg2.connect(user="postgres", password="superuser", host="192.168.132.18",
#                            port="5432", database="HAIG_Viasat_CT",sslmode="disable", gssencmode="disable")
#    return(conn)


def connect_HAIG_Viasat_CT():
    #Connect to an existing database
    conn = psycopg2.connect(user="postgres", password="superuser", host="10.0.0.1",
                            port="5432", database="HAIG_Viasat_CT", sslmode="disable", gssencmode="disable")
    return (conn)



'''
def connect_HAIG_Viasat_CT():
   #Connect to an existing database
   conn = psycopg2.connect(user="postgres", password="vaxcrio1", host="192.168.134.43",
                            port="5432", database="HAIG_Viasat_CT",sslmode="disable", gssencmode="disable")
   return(conn)
'''


def connect_HAIG_Viasat_SA():
    ##Connect to an existing database
     conn = psycopg2.connect(user="postgres", password="superuser", host="192.168.132.18",
                             port="5432", database="HAIG_Viasat_SA")
     return(conn)


def connect_HAIG_Viasat_BS():
    #Connect to an existing database
    conn = psycopg2.connect(user="postgres", password="superuser", host="10.0.0.1",
                            port="5432", database="HAIG_Viasat_BS")
    return (conn)

def connect_HAIG_Viasat_RM_2019():
    #Connect to an existing database
    conn = psycopg2.connect(user="postgres", password="superuser", host="10.0.0.1",
                            port="5432", database="HAIG_Viasat_RM_2019")
    return (conn)


### PostgreSQL on OFFICE PC ##########################################################
def connect_fede_viasat():
    # Connect to an existing database
    conn = psycopg2.connect(user="postgres", password="vaxcrio1", host="192.168.134.43",
                            port="5432", database="fede_viasat", sslmode="disable", gssencmode="disable")
    return (conn)
