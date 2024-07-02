from start_req.setup import Setup
from data_acquisition.acquisition import DataAcquisition
from data_normalization.normalizer import DataNormalization
from data_verification.verifier import DataVerification
from database_manager.db_manager import DBManager
from indicators.indicator_calculator import IndicatorCalculator
from api.api_server import APIServer


class Control:
    def __init__(self):
        self.setup = Setup()
        self.data_acquisition = DataAcquisition()
        self.data_normalization = DataNormalization()
        self.db_manager = DBManager()
        self.data_verification = DataVerification()
        self.indicator_calculator = IndicatorCalculator()
        self.api_server = APIServer()

    def initialize_system(self):
        self.setup.create_database()
        self.setup.set_up_settings()

    def acquire_and_store_data(self):
        raw_data = self.data_acquisition.get_historical_data()
        normalized_data = self.data_normalization.normalize(raw_data)
        self.db_manager.insert_data(normalized_data)
        self.data_verification.check_and_recover_data()

    def start_realtime_data_collection(self):
        self.data_acquisition.start_realtime_data()

    def calculate_indicators(self):
        data = self.db_manager.fetch_data()
        indicators = self.indicator_calculator.calculate(data)
        self.db_manager.insert_data(indicators)

    def run_api_server(self):
        self.api_server.start()

    def run_all(self):
        self.initialize_system()
        self.acquire_and_store_data()
        self.start_realtime_data_collection()
        self.calculate_indicators()
        self.run_api_server()
