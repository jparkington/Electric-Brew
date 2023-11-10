from utils import *

def create_electric_brew_db(db_path : str = "./data/sql/electric_brew.db"):
    '''
    This function creates a database and tables according to a predefined schema.
    If the database already exists, it will replace the existing tables.

    Parameters:
        db_path (str) : The path where the database file should be created.
    '''
    
    engine   = create_engine(f'sqlite:///{db_path}')
    metadata = MetaData()

    Table('meter_usage', metadata,
          Column('service_point_id',          BigInteger),
          Column('meter_id',                  String),
          Column('interval_end_datetime',     DateTime),
          Column('meter_channel',             BigInteger),
          Column('kwh',                       Float),
          Column('account_number',            String))

    Table('locations', metadata,
          Column('street',                    String),
          Column('label',                     String),
          Column('account_number',            String))

    Table('cmp_bills', metadata,
          Column('supplier',                  String),
          Column('amount_due',                Float),
          Column('service_charge',            Float),
          Column('delivery_rate',             Float),
          Column('supply_rate',               Float),
          Column('interval_start',            DateTime),
          Column('interval_end',              DateTime),
          Column('kwh_delivered',             Float),
          Column('total_kwh',                 BigInteger),
          Column('pdf_file_name',             String),
          Column('account_number',            String))

    Table('dim_datetimes', metadata,
          Column('id',                        BigInteger, primary_key = True),
          Column('timestamp',                 DateTime),
          Column('increment',                 Integer),
          Column('hour',                      Integer),
          Column('date',                      DateTime),
          Column('week',                      Integer),
          Column('week_in_year',              Integer),
          Column('month',                     Integer),
          Column('month_name',                String),
          Column('quarter',                   Integer),
          Column('year',                      Integer),
          Column('period',                    String))

    Table('dim_meters', metadata,
          Column('id',                        BigInteger, primary_key = True),
          Column('meter_id',                  String),
          Column('service_point_id',          BigInteger),
          Column('account_number',            String),
          Column('street',                    String),
          Column('label',                     String))

    Table('dim_suppliers', metadata,
          Column('id',                        BigInteger, primary_key = True),
          Column('supplier',                  String),
          Column('avg_supply_rate',           Float))

    Table('fct_electric_brew', metadata,
          Column('id',                        BigInteger, primary_key = True),
          Column('dim_datetimes_id',          BigInteger, ForeignKey('dim_datetimes.id')),
          Column('dim_meters_id',             BigInteger, ForeignKey('dim_meters.id')),
          Column('dim_suppliers_id',          BigInteger, ForeignKey('dim_suppliers.id')),
          Column('kwh',                       Float),
          Column('service_charge',            Float),
          Column('delivery_rate',             Float),
          Column('supply_rate',               Float),
          Column('allocated_service_charge',  Float),
          Column('delivered_kwh_left',        Float),
          Column('delivered_kwh_used',        Float),
          Column('total_cost_of_delivery',    Float),
          Column('account_number',            String))

    try:
        metadata.create_all(engine)

        lg.info(f"Database and tables created at {db_path}")

    except SQLAlchemyError as e:
        lg.error(f"Error creating database and tables: {e}")