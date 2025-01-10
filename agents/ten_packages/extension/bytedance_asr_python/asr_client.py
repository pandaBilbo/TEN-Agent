import asyncio
import base64
import gzip
import hmac
import json
import uuid
import websockets
from hashlib import sha256
from urllib.parse import urlparse

PROTOCOL_VERSION = 0b0001
DEFAULT_HEADER_SIZE = 0b0001

CLIENT_FULL_REQUEST = 0b0001
CLIENT_AUDIO_ONLY_REQUEST = 0b0010
SERVER_FULL_RESPONSE = 0b1001
SERVER_ACK = 0b1011
SERVER_ERROR_RESPONSE = 0b1111

NO_SEQUENCE = 0b0000
POS_SEQUENCE = 0b0001
NEG_SEQUENCE = 0b0010
NEG_SEQUENCE_1 = 0b0011

NO_SERIALIZATION = 0b0000
JSON = 0b0001
THRIFT = 0b0011
CUSTOM_TYPE = 0b1111

NO_COMPRESSION = 0b0000
GZIP = 0b0001
CUSTOM_COMPRESSION = 0b1111

def generate_header(
    version=PROTOCOL_VERSION,
    message_type=CLIENT_FULL_REQUEST,
    message_type_specific_flags=NO_SEQUENCE,
    serial_method=JSON,
    compression_type=GZIP,
    reserved_data=0x00,
    extension_header=bytes()
):
    header = bytearray()
    header_size = int(len(extension_header) / 4) + 1
    header.append((version << 4) | header_size)
    header.append((message_type << 4) | message_type_specific_flags)
    header.append((serial_method << 4) | compression_type)
    header.append(reserved_data)
    header.extend(extension_header)
    return header

def generate_full_default_header():
    return generate_header()

def generate_audio_default_header():
    return generate_header(message_type=CLIENT_AUDIO_ONLY_REQUEST)

def generate_last_audio_default_header():
    return generate_header(
        message_type=CLIENT_AUDIO_ONLY_REQUEST,
        message_type_specific_flags=NEG_SEQUENCE
    )

def parse_response(res):
    protocol_version = res[0] >> 4
    header_size = res[0] & 0x0f
    message_type = res[1] >> 4
    message_type_specific_flags = res[1] & 0x0f
    serialization_method = res[2] >> 4
    message_compression = res[2] & 0x0f
    reserved = res[3]
    header_extensions = res[4:header_size * 4]
    payload = res[header_size * 4:]
    result = {}
    payload_msg = None
    payload_size = 0
    
    if message_type == SERVER_FULL_RESPONSE:
        payload_size = int.from_bytes(payload[:4], "big", signed=True)
        payload_msg = payload[4:]
    elif message_type == SERVER_ACK:
        seq = int.from_bytes(payload[:4], "big", signed=True)
        result['seq'] = seq
        if len(payload) >= 8:
            payload_size = int.from_bytes(payload[4:8], "big", signed=False)
            payload_msg = payload[8:]
    elif message_type == SERVER_ERROR_RESPONSE:
        code = int.from_bytes(payload[:4], "big", signed=False)
        result['code'] = code
        payload_size = int.from_bytes(payload[4:8], "big", signed=False)
        payload_msg = payload[8:]
        
    if payload_msg is None:
        return result
        
    if message_compression == GZIP:
        payload_msg = gzip.decompress(payload_msg)
    if serialization_method == JSON:
        payload_msg = json.loads(str(payload_msg, "utf-8"))
    elif serialization_method != NO_SERIALIZATION:
        payload_msg = str(payload_msg, "utf-8")
        
    result['payload_msg'] = payload_msg
    result['payload_size'] = payload_size
    return result

class AsrWsClient:
    def __init__(self, cluster, **kwargs):
        self.cluster = cluster
        self.success_code = 1000
        self.seg_duration = int(kwargs.get("seg_duration", 15000))
        self.nbest = int(kwargs.get("nbest", 1))
        self.appid = kwargs.get("appid", "")
        self.token = kwargs.get("token", "")
        self.ws_url = kwargs.get("ws_url", "wss://openspeech.bytedance.com/api/v2/asr")
        self.uid = kwargs.get("uid", "streaming_asr_demo")
        self.workflow = kwargs.get("workflow", "audio_in,resample,partition,vad,fe,decode,itn,nlu_punctuate")
        self.show_language = kwargs.get("show_language", False)
        self.show_utterances = kwargs.get("show_utterances", True)
        self.result_type = kwargs.get("result_type", "single")
        self.format = kwargs.get("format", "pcm")
        self.rate = kwargs.get("sample_rate", 16000)
        self.language = kwargs.get("language", "zh-CN")
        self.bits = kwargs.get("bits", 16)
        self.channel = kwargs.get("channel", 1)
        self.codec = kwargs.get("codec", "raw")
        self.auth_method = kwargs.get("auth_method", "token")
        self.secret = kwargs.get("secret", "access_secret")

    def construct_request(self, reqid):
        return {
            'app': {
                'appid': self.appid,
                'cluster': self.cluster,
                'token': self.token,
            },
            'user': {
                'uid': self.uid
            },
            'request': {
                'reqid': reqid,
                'nbest': self.nbest,
                'workflow': self.workflow,
                'show_language': self.show_language,
                'show_utterances': self.show_utterances,
                'result_type': self.result_type,
                "sequence": 1
            },
            'audio': {
                'format': "pcm",
                'rate': self.rate,
                'language': self.language,
                'bits': self.bits,
                'channel': self.channel,
                'codec': self.codec
            }
        }

    @staticmethod
    def slice_data(data: bytes, chunk_size: int):
        data_len = len(data)
        offset = 0
        while offset + chunk_size < data_len:
            yield data[offset: offset + chunk_size], False
            offset += chunk_size
        else:
            yield data[offset: data_len], True

    def token_auth(self):
        return {'Authorization': 'Bearer; {}'.format(self.token)}

    def signature_auth(self, data):
        header_dicts = {'Custom': 'auth_custom'}
        url_parse = urlparse(self.ws_url)
        input_str = 'GET {} HTTP/1.1\n'.format(url_parse.path)
        auth_headers = 'Custom'
        for header in auth_headers.split(','):
            input_str += '{}\n'.format(header_dicts[header])
        input_data = bytearray(input_str, 'utf-8')
        input_data += data
        mac = base64.urlsafe_b64encode(
            hmac.new(self.secret.encode('utf-8'), input_data, digestmod=sha256).digest())
        header_dicts['Authorization'] = 'HMAC256; access_token="{}"; mac="{}"; h="{}"'.format(
            self.token, str(mac, 'utf-8'), auth_headers)
        return header_dicts

class ByteDanceWebSocketClient:
    def __init__(self, config):
        self.ws = None
        self.client = AsrWsClient(
            cluster=config.cluster,
            appid=config.api_key,
            token=config.token,
            sample_rate=config.sample_rate,
            bits=config.bits,
            channel=config.channels,
        )
        self.callbacks = {}
        self.connected = False
        self.reconnecting = False

    async def start(self):
        try:
            reqid = str(uuid.uuid4())
            request_params = self.client.construct_request(reqid)
            payload_bytes = gzip.compress(str.encode(json.dumps(request_params)))
            
            full_client_request = bytearray(generate_full_default_header())
            full_client_request.extend((len(payload_bytes)).to_bytes(4, 'big'))
            full_client_request.extend(payload_bytes)
            
            headers = self.client.token_auth()
            if self.ws:
                await self.ws.close()
                
            self.ws = await websockets.connect(
                self.client.ws_url,
                extra_headers=headers,
                max_size=1000000000
            )
            
            await self.ws.send(full_client_request)
            self.connected = True
            
            if self.callbacks.get('open'):
                await self.callbacks['open']()
                
            asyncio.create_task(self._listen())
            return True
        except Exception as e:
            if self.callbacks.get('error'):
                await self.callbacks['error'](str(e))
            return False

    async def send(self, audio_data: bytes):
        if not self.connected:
            return False
            
        try:
            payload_bytes = gzip.compress(audio_data)
            audio_only_request = bytearray(generate_audio_default_header())
            audio_only_request.extend((len(payload_bytes)).to_bytes(4, 'big'))
            audio_only_request.extend(payload_bytes)
            
            await self.ws.send(audio_only_request)
            return True
        except Exception:
            self.connected = False
            return False

    async def _listen(self):
        while self.connected:
            try:
                res = await self.ws.recv()
                result = parse_response(res)
                
                if result and 'payload_msg' in result:
                    payload = result['payload_msg']
                    if payload.get('code') != 1000:
                        if self.callbacks.get('error'):
                            await self.callbacks['error'](payload)
                        continue
                        
                    if 'result' in payload:
                        if self.callbacks.get('result'):
                            await self.callbacks['result'](payload)
            except Exception as e:
                self.connected = False
                if self.callbacks.get('close'):
                    await self.callbacks['close']()
                break

    def on(self, event: str, callback):
        self.callbacks[event] = callback

    async def close(self):
        self.connected = False
        if self.ws:
            await self.ws.close()
            self.ws = None 