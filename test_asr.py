#!/usr/bin/env python3

# Usage:
# pip install python-dotenv
# python3 test_asr.py

import os
import glob
import subprocess
import time
import logging
from dotenv import load_dotenv
from datetime import datetime
import json
import re


class AsrTester:
    def __init__(
        self,
        interval_seconds=20,
        log_dir="/app/tests_asr_logs",
        pcm_dir="/app/tests_asr_pcm_en",
        # pcm_dir="/app/tests_asr_pcm",
        property_json="/app/tests_asr_property/test_asr_%s_property.json",
        # round=5,
        round=1,
    ):
        self.batch_number = datetime.now().strftime("%Y%m%d_%H%M")
        self.interval_seconds = interval_seconds
        self.pcm_dir = pcm_dir
        self.pcm_files = []
        self.process = None
        self.property_json = property_json
        self.round = round
        # self.vendors = ["aliyun", "baidu", "bytedance", "tencent"]
        # self.vendors = ["baidu", "bytedance", "tencent"]
        # self.vendors = ["aliyun"]
        # Overseas vendors
        # self.vendors = ["deepgram", "soniox"]
        self.vendors = ["soniox"]
        # self.vendors = ["deepgram"]

        # Create logs directory if it doesn't exist
        self.logs_dir = "%s/tests_asr_logs_%s" % (log_dir, self.batch_number)
        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)

        # Set up logging
        self.setup_logging()

        # Load environment variables from .env file
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        if os.path.exists(env_path):
            load_dotenv(env_path)
            logging.info(f"Loaded environment variables from {env_path}")
        else:
            logging.warning(f".env file not found at {env_path}")

    def clean_timestamp(self, timestamp):
        """Clean the timestamp string by removing unwanted characters."""
        """Remove special characters from the text."""
        return re.sub(r"\x1b\[[0-?9;]*[mK]", "", timestamp)

    def get_property_json(self, vendor):
        return self.property_json % vendor

    def replace_property_json(self, pcm_file, property_json):
        with open(property_json, "r") as file:
            json_content = json.load(file)

        json_content["_ten"]["predefined_graphs"][0]["nodes"][1]["property"][
            "pcm_file"
        ] = pcm_file

        with open(property_json, "w") as file:
            json.dump(json_content, file, indent=4)

    def setup_logging(self):
        """Set up logging configuration"""
        # Create log file
        log_file = os.path.join(
            self.logs_dir, f"asr_test_script_{self.batch_number}.log"
        )

        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(process)d - [%(filename)s:%(lineno)d] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
        )

        # Log initial message
        logging.info(f"ASR Test Started, log_file: {log_file}")
        logging.info("-" * 50)

    def scan_files(self):
        """Scan for PCM files in the directory"""
        # self.pcm_files = glob.glob(os.path.join(self.pcm_dir, "*.pcm"))
        # TODO
        # self.pcm_files = glob.glob(os.path.join(self.pcm_dir, "Cn_01.pcm"))
        self.pcm_files = glob.glob(os.path.join(self.pcm_dir, "*_01.pcm"))
        if not self.pcm_files:
            logging.warning(f"No PCM files found in {self.pcm_dir}")
            return False

        logging.info(f"Found {len(self.pcm_files)} PCM files:")
        for file in self.pcm_files:
            logging.info(f"- {os.path.basename(file)}")
        return True

    def get_pcm_files(self):
        """Get the list of PCM files"""
        return self.pcm_files

    def get_worker_log_file(self, pcm_file, vendor, round):
        """Get worker logs file"""
        return os.path.join(
            self.logs_dir,
            "asr_test_worker_%s_%s_%s_%d.log"
            % (self.batch_number, os.path.basename(pcm_file), vendor, round),
        )

    def process_pcm_file(self, pcm_file, vendor, round):
        """Process a single PCM file"""
        try:
            logging.info(
                f"Processing file: {pcm_file} for vendor: {vendor}, round: {round}"
            )

            # Start ASR service for this file
            if not self.start_asr_service(pcm_file, vendor, round):
                return

        except Exception as e:
            logging.error(f"Error processing file {pcm_file}: {e}")
        finally:
            # Stop ASR service after processing
            self.stop_asr_service()
            # Wait before processing next file
            time.sleep(1)

    def start_asr_service(self, pcm_file, vendor, round):
        """Start the ASR service"""
        try:
            property_json = self.get_property_json(vendor)
            self.replace_property_json(pcm_file, property_json)

            command = (
                f"cd /app/agents && /app/agents/bin/start --property {property_json}"
            )
            logging.info(f"Starting ASR service with command: {command}")

            worker_log_file = self.get_worker_log_file(pcm_file, vendor, round)
            with open(worker_log_file, "w", encoding="utf-8") as output_file:
                self.process = subprocess.Popen(
                    command,
                    shell=True,
                    stdout=output_file,
                    stderr=output_file,
                    text=True,
                )
            logging.info(f"ASR service started, worker_log_file: {worker_log_file}")

            # Wait for service to initialize
            time.sleep(10)
            return True
        except Exception as e:
            logging.error(f"Failed to start ASR service: {e}")
            return False

    def stop_asr_service(self):
        """Stop the ASR service"""
        if self.process:
            try:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()

                stdout, stderr = self.process.communicate()
                if stdout:
                    logging.info("Service output:")
                    for line in stdout.splitlines():
                        logging.info(f"  {line}")
                if stderr:
                    logging.error("Service errors:")
                    for line in stderr.splitlines():
                        logging.error(f"  {line}")

                logging.info("ASR service stopped")
            except Exception as e:
                logging.error(f"Error stopping ASR service: {e}")
            finally:
                self.process = None

    def run(self):
        """Run the ASR testing process"""
        logging.info(f"ASR Run Started")
        logging.info("-" * 50)

        if not self.scan_files():
            return

        for pcm_file in self.pcm_files:
            for vendor in self.vendors:
                for i in range(self.round):
                    worker_log_file = self.get_worker_log_file(pcm_file, vendor, i)

                    logging.info(
                        f"Process round {i}, worker_log_file: {worker_log_file}"
                    )

                    self.process_pcm_file(pcm_file, vendor, i)

                    # Wait for processing
                    logging.info(
                        f"Waiting for {self.interval_seconds} seconds before processing next file"
                    )
                    time.sleep(self.interval_seconds)

        logging.info("All files processed")
        logging.info(f"ASR Run Ended")

    def stat(self):
        """Stat the ASR testing process"""
        logging.info(f"ASR Stat Started")
        logging.info("-" * 50)

        # if not self.scan_files():
        #     return

        stat_csv_file = os.path.join(
            self.logs_dir, f"asr_test_stat_{self.batch_number}.csv"
        )
        logging.info(f"stat_csv_file: {stat_csv_file}")

        with open(stat_csv_file, "w") as csv_file:
            csv_file.write("pcm_file,vendor,ts_start,ts_end,time_duration(ms)\n")

            for pcm_file in self.pcm_files:
                for vendor in self.vendors:
                    for i in range(self.round):
                        worker_log_file = self.get_worker_log_file(pcm_file, vendor, i)

                        logging.info(
                            f"Stat round {i}, worker_log_file: {worker_log_file}"
                        )

                        ts_start = 0
                        ts_end = 0
                        point_on_audio_frame_first_found = False
                        try:
                            with open(worker_log_file, "r") as file:
                                for line in file:
                                    if "ASR_TEST_POINT_ON_AUDIO_FRAME" in line:
                                        if point_on_audio_frame_first_found:
                                            continue

                                        point_on_audio_frame_first_found = True

                                        parts = line.split(
                                            "ASR_TEST_POINT_ON_AUDIO_FRAME:"
                                        )
                                        if len(parts) > 1:
                                            ts_start = self.clean_timestamp(
                                                parts[1].strip()
                                            )

                                        logging.info(
                                            f"Find ASR_TEST_POINT_ON_AUDIO_FRAME, worker_log_file: {worker_log_file}, ts_start: {ts_start}, line: {line}"
                                        )

                                    if "ASR_TEST_POINT_IS_FINAL" in line:
                                        parts = line.split("ASR_TEST_POINT_IS_FINAL:")
                                        if len(parts) > 1:
                                            ts_end = self.clean_timestamp(
                                                parts[1].strip()
                                            )

                                        logging.info(
                                            f"Find ASR_TEST_POINT_IS_FINAL, worker_log_file: {worker_log_file}, ts_end: {ts_end}, line: {line}"
                                        )

                            time_diff = int(ts_end) - int(ts_start)
                            logging.info(
                                f"Time duration: {time_diff} ms, worker_log_file: {worker_log_file}, pcm_file: {pcm_file}, vendor: {vendor}"
                            )

                            csv_file.write(
                                "%s,%s,%s,%s,%s\n"
                                % (
                                    os.path.basename(pcm_file),
                                    vendor,
                                    ts_start,
                                    ts_end,
                                    time_diff,
                                )
                            )
                        except Exception as e:
                            logging.error(
                                f"Error reading log file {worker_log_file}: {e}"
                            )

        logging.info(f"All files stated, stat_csv_file: {stat_csv_file}")
        logging.info(f"ASR Stat Ended")


if __name__ == "__main__":
    asr_tester = AsrTester()
    asr_tester.run()
    asr_tester.stat()

