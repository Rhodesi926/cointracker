import requests
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import json
import os
import time
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables
load_dotenv()

class KOLTracker:
    def __init__(self):
        self.helius_api_key = os.getenv('HELIUS_API_KEY')
        
        if not self.helius_api_key:
            raise ValueError("HELIUS_API_KEY not found in environment variables")
        
        print(f"Using Helius API key: {self.helius_api_key[:8]}...")
        self.helius_url = "https://api.helius.xyz/v0"


        

        self.kol_wallets = {
            'BGA6DoNa6aJzRWWt1CGoF2GLtNdf2sN5s9eHi1Tgcjas': {'name': 'p1', 'twitter': '@p1'},
            '5xK1SVsWJ89Y1yNSPKRckkB5xzisRFoCFUFsaKsYAqm': {'name': 'p2', 'twitter': '@p2'},
            '4KPW4Gx9jcBcBpEXwAbx7WwLc4R6KQzTbTeWWFrk2HBr': {'name': 'p3', 'twitter': '@p3'},
            '3xRp3nT2Jndh9YecBJqgQtyyjSC82rBcyPuor3PsumxR': {'name': 'p4', 'twitter': '@p4'},
            '94EeAexcdVStbA8MHwVujkNkUpDD5sWKpLiSDDtyps6H': {'name': 'p5', 'twitter': '@p5'},
            '3kShLyrTSz6NYc2XBHq6RVoMA4Hh8g39Bxrdf3Ki45TG': {'name': 'p6', 'twitter': '@p6'},
            '9FfUy36GDbPubNLeCWvZGubTc71G3aGwfRwZVo38Zf1q': {'name': 'p7', 'twitter': '@p7'},
            '2A9jtVhMh6tF9cGZ2FXEDCP7rSDXgeeQrL1RzTs81wFC': {'name': 'p8', 'twitter': '@p8'},
            'Hq8MmCBFavX2GooSCk9XFp4Whue3wmC3jaZqk1zDgSXx': {'name': 'p9', 'twitter': '@p9'},
            '9ufnH3CRMGqFb1LHoeNF15ueXNGqKqA1PqK2QvBwsAuW': {'name': 'p10', 'twitter': '@p10'},
            '664yPUmgPqFcjTjCsRQRXZ4rAKDkF2vSwhyndeBgsYJy': {'name': 'p11', 'twitter': '@p11'},
            '2ymS3hUFJWAKUGMUnCJpLge5NoydGEc9J5HQ3zKSVHL2': {'name': 'p12', 'twitter': '@p12'},
            'DrD3N4TCLzkTibvbJzYLz79jy4m7h1ZKzUbLnf63acXq': {'name': 'p13', 'twitter': '@p13'},
            'EqGa7KmAnTwkDimrgtgBG8rVX8kpHs6Sp9dATeVpJbXU': {'name': 'p14', 'twitter': '@p14'},
            '8rUbDTQYuZMJqdgMXDEXK5bYFNHbLkTSECJbAX1J5W4U': {'name': 'p15', 'twitter': '@p15'},
            'BpegTnFHz3UVbZjw5bNQkUDnKfyKwQtxgAXWZGfU3zFZ': {'name': 'p16', 'twitter': '@p16'},
            'DXfKLhJGPxErhNDnj5BFX9tciz4e3RJxrUrkumj1DuvR': {'name': 'p17', 'twitter': '@p17'},
            '5yrRrU9RXRSMjwAfQcLiaAh5Lfwtu2WrA6gDZowU2v2q': {'name': 'p18', 'twitter': '@p18'},
            'Eb1kkLk6VxE6vE7iUfQ9xbaXkmhjfZyqL4zgUrbNAgWG': {'name': 'p19', 'twitter': '@p19'},
            '6uwqG7Uog6FVUHZp5rx6jkJ8VjWjNf7svcAzzcCJQh7Q': {'name': 'p20', 'twitter': '@p20'},
            '7uCGCcEx1kfSCVB5hHEqfGi26bR7NY9vL8QN18ys6BQw': {'name': 'p21', 'twitter': '@p21'},
            'JACRrUafnGPGsUcUTJ9xo9DjUdXhyyG5fBraSP4isgZT': {'name': 'p22', 'twitter': '@p22'},
            'HJmvnbmi5dp2ZAN7Sms3HGUUBpfLrxHAYiFNpGGa47Mf': {'name': 'p23', 'twitter': '@p23'},
            '5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9': {'name': 'p24', 'twitter': '@p24'},
            'DfLhfARwzJGN6U3CEPm3HSJi4g6is3p9uYEnv7f6Zqj6': {'name': 'p25', 'twitter': '@p25'},
            '8gwBuaJncAfVyb4Fi2AwFAHvatWYDrEpcXUJJa5mCwRs': {'name': 'p26', 'twitter': '@p26'},
            '4aMUeBL6aVCrc8gxDiAVTRAVqWhj9Fpv12N3aJZfQ13X': {'name': 'p27', 'twitter': '@p27'},
            '8g75dYdnA2uifiS3KRusWHueBZ2ikyWXHvJUZP8AbCBq': {'name': 'p28', 'twitter': '@p28'},
            'MfDuWeqSHEqTFVYZ7LoexgAK9dxk7cy4DFJWjWMGVWa': {'name': 'p29', 'twitter': '@p29'},
            'GLi418odqLgLETe6eWmoD3tGYrbjRBekPEWjUV2HQNr': {'name': 'p30', 'twitter': '@p30'},
            'BeysXhUunQPK8hRbfwumWiLKyTJnjs3J1gY1mCrjM7z1': {'name': 'p31', 'twitter': '@p31'},
            'uq47b7vZcXu4egzHUjSP39yWoJgERwwwNEUdZVXq6YA': {'name': 'p32', 'twitter': '@p32'},
            'AmManpABYP2yxskBmt31VSrALjCnTyaEdK8qeSVWLmjs': {'name': 'p33', 'twitter': '@p33'},
            'CH9i39Jga5ZTsKKadFd4N9V3GUUXjn3gPxJhePTeoJ22': {'name': 'p34', 'twitter': '@p34'},
            '7BrJ34cn4H1oyGauU2KhcrzYFUEJG3Y5Nj81n7kz6Ncq': {'name': 'p35', 'twitter': '@p35'},
            '5nThBegYDJvsTUyAgveJxyWvnbNoHMTw7RYpf4osWNnR': {'name': 'p36', 'twitter': '@p36'},
            '6Ji9sf7ixzAM7u1V2ZntZAu69vLHFp9r38K2HtEPzD1U': {'name': 'p37', 'twitter': '@p37'},
            '7MxGXBDAgWWdyeoXk7ii9QMcgUrTPcfn11gGiiQSH3uf': {'name': 'p38', 'twitter': '@p38'},
            'GMWjyRxW5zZqH1DtFYwSubi1LgiczLucwWMhsfVFSDJh': {'name': 'p39', 'twitter': '@p39'},
            '9HTA8PwuB9ck5R9d6pMHdpkcSzFxAonDGBvHBhKokKPQ': {'name': 'p40', 'twitter': '@p40'},
            '2G3cYwEB3pXZyUFozbqUtYYGtcy1ZirMDQXzF7y4yVor': {'name': 'p41', 'twitter': '@p41'},
            'HU2yQ6xNoNoAW9XCeqfsdQVFeJYXrx2LZNbjMuEre2tz': {'name': 'p42', 'twitter': '@p42'},
            'AQPSSiegWdoDWDLt37RhCmT6JJdH8mB12svn4uTLg1bo': {'name': 'p43', 'twitter': '@p43'},
            '7GqPydM9jNCEAcA37kts847ZZ5Co1EiD6F2R3ExmS7Dp': {'name': 'p44', 'twitter': '@p44'},
            'EJgNC31hmUESQwds8MZ1rH3MYPm3JkbKpCJ1ujE8V5Q5': {'name': 'p45', 'twitter': '@p45'},
            '3g4dw158Pvr68TBrWiPrcss8Xwo2zowTLX3LuVH3hGhA': {'name': 'p46', 'twitter': '@p46'},
            'Y1pWip99gZEmrxeaBmmqgzYEKpMWrdrsFr4VXBACBU4': {'name': 'p47', 'twitter': '@p47'},
            '5gR9AHKrPrwrLzqWHdmxrN9ba9PXKQcTuvoVRXX1G8XE': {'name': 'p48', 'twitter': '@p48'},
            'FihDWTiruvNHJyarCcwgAP9Z8FthXfBeZ2FncEnA4MUS': {'name': 'p49', 'twitter': '@p49'},
            'HTHGnAZkp7NZrqQs3VVL5ynjL1F1CujHLGdYpK6VtDfZ': {'name': 'p50', 'twitter': '@p50'},
            '9GMC2wzp4c7okeVzUT6vsc517YyocC5JfDUb1jPyJ6Up': {'name': 'p51', 'twitter': '@p51'},
            '61BTiqBSztTE46dy6qPCLftNsdCuAyEe48fHnUBJp3tY': {'name': 'p52', 'twitter': '@p52'},
            'MSm3jYb4WVgW49Ctf3NWnYQixqMz2sW1eiE8zfo7nLF': {'name': 'p53', 'twitter': '@p53'},
            '7RoMjXXE88PoFhUVjmF6i5Er1qYM9iX2igAzodxVSeUR': {'name': 'p54', 'twitter': '@p54'},
            'DzssMBG83ySNoizUpGVLvKmvTtzGRf9KFwBQbE15nYcL': {'name': 'p55', 'twitter': '@p55'},
            'CJbtcwcLqzHc4oW63rhfmrAxvL2ZbvgJYDPANGV5YaPt': {'name': 'p56', 'twitter': '@p56'},
            'BDzu84TVpqEC883KbxSThgBykFV8JFoc2qhfi3KQVp6J': {'name': 'p57', 'twitter': '@p57'},
            'BDFMkurHjWM8HCibLRncxaSF8eQ3oJJztPSr4jUEo3nZ': {'name': 'p58', 'twitter': '@p58'},
            '8i57XsS3E4iuw2qy2cPbKDWnW4pwx6yaBc7N7UQzG3MJ': {'name': 'p59', 'twitter': '@p59'},
            '3eJkwFDZVB27emciij1oWUVodmFhFdnkpmzKHjDzH34o': {'name': 'p60', 'twitter': '@p60'},
            'Ds5jCefMDMCbwjrypDVqUvXn3wqTgHyvwD6iCrJYkWYy': {'name': 'p61', 'twitter': '@p61'},
            '2CR6zpd28GWaQgc8w5hn2gtCbiVzn7ifLhzu4rAmTtDs': {'name': 'p62', 'twitter': '@p62'},
            'E65QjbAxG8ZMoCGt1ChkT8e6MmSeikF2jNZbqnD3ZdiM': {'name': 'p63', 'twitter': '@p63'},
            '7uQxuvVFzY3uApna9PFQEMQEUKrrwFYG4D4272aPHT1d': {'name': 'p64', 'twitter': '@p64'},
            '6Ru8fGXkZCVY1bFoc7dg1Mtt6qyCdESeb9BkRUetqUS7': {'name': 'p65', 'twitter': '@p65'},
            'BbumVEtBca2YVHcPtb8ZNENX9c3mYnPTK9vN62mZ8bnX': {'name': 'p66', 'twitter': '@p66'},
            'Hz3UXqdDtJyV2tk8y7jNvL8bDxYwmSUGpHS4Rg9G1Dk8': {'name': 'p67', 'twitter': '@p67'},
            '4nZ263ccsQj1monDZPMqeHuNaFcitUKhLtWRyGUoGCUZ': {'name': 'p68', 'twitter': '@p68'},
            'BDzury9AA1zKee7gcDnAxruRF9AomgiMLioZYxGnVp6J': {'name': 'p69', 'twitter': '@p69'},
            '2CR6jG5ytiUT1yftSuEz7n1S3mi7oczThN9vA2u6TtDs': {'name': 'p70', 'twitter': '@p70'},
            'HxrAFWBpBFZGWrAxNM2LD66GXjPKcxz8uB6HwtYU3TM7': {'name': 'p71', 'twitter': '@p71'},
            'BMnT51N4iSNhWU5PyFFgWwFvN1jgaiiDr9ZHgnkm3iLJ': {'name': 'p72', 'twitter': '@p72'},
            'EMKbfGCxqtMSjRMPyt7MGWd2zm8WGppvVZAphRUhbzEC': {'name': 'p73', 'twitter': '@p73'},
            'FPDB4tEyq1FTCqu83mBe73nC3GsLM8SNLm8t44EU3oxf': {'name': 'p74', 'twitter': '@p74'},
            'DaJMBi7jdMuXGPWzy4zeERrtpjUKesz9CbnfDerVbY2W': {'name': 'p75', 'twitter': '@p75'},
            '8jqpi2pRyYb9L79ChXp8Zp4HEKEAJidseZqtRKZWfUNf': {'name': 'p76', 'twitter': '@p76'},
            'HQ4JojrjnMegW35Y8fk5xqEUjiuPn6YhKrvjwNM48ZbC': {'name': 'p77', 'twitter': '@p77'},
            '5vTmma3Ay6w3xoC1B1Dstfwtwdvxp8Wmw4Ymqmm2QWAN': {'name': 'p78', 'twitter': '@p78'},
            'FzSFLzPY3Ame3TbeFgr8KAmt4JFhVWFFe9AWPrRHd1rq': {'name': 'p79', 'twitter': '@p79'},
            'Hz73rG8zVjvdCHbKzi2GE3Sx3yDfRQzAb8eRg4YSVCte': {'name': 'p80', 'twitter': '@p80'},
            'GnSC2SNdHbXk8hXWQqYEWgasGb2CY3mpTB3nyXU8DyEN': {'name': 'p81', 'twitter': '@p81'},
            'G6tcDjqi3De3rDjUHS3FvwP3Pvqj2hzwDGhwkGML6vJT': {'name': 'p82', 'twitter': '@p82'},
            'BiESLRE2R32eD2r89v1HQrXAN5gCFNfgr6G1KGiDDX46': {'name': 'p83', 'twitter': '@p83'},
            'CcTNKBhQKYu7nB4eRrRFQoXEvpd6H5QnGKh93pwniBtp': {'name': 'p84', 'twitter': '@p84'},
            '4gQT88rvHr6ay8XvmUUriL5FTjsSuvkvH8ybz3etVLBb': {'name': 'p85', 'twitter': '@p85'},
            '6HRgJMRmjaj2svP9GpRbUU5TPzLAHnBW3sHgYVbirWYE': {'name': 'p86', 'twitter': '@p86'},
            'bKaQgj9UoihD45UXEaKGTodwop4iyKz5NRQcxFGvDCx': {'name': 'p87', 'twitter': '@p87'},
            'ExfKQ3W72wDdrxY1XWduCmmfrxHu3CDRBeRuNAp5qKdS': {'name': 'p88', 'twitter': '@p88'},
            '5XajeMJJF8XCmz4zHETx9T3793driM9CMQUU7KdmnCvA': {'name': 'p89', 'twitter': '@p89'},
            '1NKvBXJBftBvph3m2t2u5hyWWMusUYQ6Wj9KLsGziyE': {'name': 'p90', 'twitter': '@p90'},
            'AvJE5uoCbfFivo4avxfm6LAmyCb2eqEUqDrJQ1bd43np': {'name': 'p91', 'twitter': '@p91'},
            'EQbcCPPYskMdEXMfkPnTwLXn9V92Pok2ZSVdFpvd9tKn': {'name': 'p92', 'twitter': '@p92'},
            'G3uePKaJXN4aTfUknrpgpwNi5gSkVkUbTxnzBBbctcaR': {'name': 'p93', 'twitter': '@p93'},
            'hJLcjeAUH6zdLVCrmF9wUz541wkhtTDkRFusckniD1t': {'name': 'p94', 'twitter': '@p94'},
            'BS7MK96z2pccc9Ep1M5zrtYoFbc7YgfY5DChRoh2kJc1': {'name': 'p95', 'twitter': '@p95'},
            '7g1nSRApscRcSpH8sZtJvprow3QvUtXS28ugZyN4466u': {'name': 'p96', 'twitter': '@p96'},
            'C1wBDEgizDWsPTtEXFvRr6fcSxcHM2zkha4ZhRz3md2A': {'name': 'p97', 'twitter': '@p97'},
            '6bWRniNsAePmxSux2iMUvkTj2Hq51dLqoCeGYasdTU9H': {'name': 'p98', 'twitter': '@p98'},
            '78BFrH5x7h3XJM5ybFRKgDaq33hUzqDjEYtHLnRLm48d': {'name': 'p99', 'twitter': '@p99'},
            'GJFcrvtxJXr7esGAFDFWSMLsnt4eFFhWQhe7KvXVPCbJ': {'name': 'p100', 'twitter': '@p100'},
            'J6dzyXEnSvy9u8Gp2MXq4V6zPX1966BWYWPkcBZ4wyog': {'name': 'p101', 'twitter': '@p101'},
            '6ZEYQvMqfBDjUskDCsoou43puWjEE3VoSWu6ebmp9Yd6': {'name': 'p102', 'twitter': '@p102'},
            'EfPrX4vKU71pSNpfWiCMmabar5MW9axTF7JeTQ6M42LE': {'name': 'p103', 'twitter': '@p103'},
            'Cs9yJGho4n2UtjnUTgFNL2CYwWuAq5gdrGgZCgiVUQcq': {'name': 'p104', 'twitter': '@p104'},
            '78FQcgPxTfJL1oFj7eh7Kbn7F864qtNLDPSvNcGmDaB2': {'name': 'p105', 'twitter': '@p105'},
            'CrHMyQyvoDGFE7grxBHgm6V5FZCpDkXdf9EaQ6EwWcd5': {'name': 'p106', 'twitter': '@p106'},
            '5yjYkGKrBkPWo8zBNnA2XkHbNV5LYQTcaSpmdGLhScS6': {'name': 'p107', 'twitter': '@p107'},
            'EmSg4dS3JVmecSVTLzRaMnpyHg5j4XXqdnaC3NNMYzys': {'name': 'p108', 'twitter': '@p108'},
            'AevgmVgQuntnx9HmsJgMPfAuxSZozVdhnk3REvN7CDeH': {'name': 'p109', 'twitter': '@p109'},
            'F2bdzJDsFcPTY7jdZXRwVwqV83ee7BLgfzX5Po8Q1Ayk': {'name': 'p110', 'twitter': '@p110'},
            'FKyU75XeYuYCSHRKZRrLPoixawJafK9g8efJFWvVopBR': {'name': 'p111', 'twitter': '@p111'},
            '41GPgzqhS2531BUxJvH5KWjvsisoG8xS32k3T2HtefNy': {'name': 'p112', 'twitter': '@p112'},
            'C45j2SRendoyksptapsviaPvS3KF6gk8SGFEy85TPXUg': {'name': 'p113', 'twitter': '@p113'},
            'HzNtkV7huNFEk33bA3Bt7hArDpjX4aDDwdfv87Hw7Gvd': {'name': 'p114', 'twitter': '@p114'},
            '2ajQtjXSWStBLt6MHcVCHAmEFHVTkWcwKtVAM8AeBnA8': {'name': 'p115', 'twitter': '@p115'},
            '34FfD7MKrKsXjdkTPsdCCWEDjHCxDPEDYh61XEU6uimR': {'name': 'p116', 'twitter': '@p116'},
            'BhP6CMpoBt76j17Z2Ne2asv9Qm3RBjYAy17Fe6AAXgYW': {'name': 'p117', 'twitter': '@p117'},
            'FoBSb6Zozju2QRsLaEAVQf6Hi8ye7ZGgSB7fuNs7T3AK': {'name': 'p118', 'twitter': '@p118'},
            'FPGT1sGVuTEkWRTuk23uyDyz8zLB4GSBZHva4LxgK6uF': {'name': 'p119', 'twitter': '@p119'},
            'HgsRiULmQGrB6eKnPUkdE4H1bYLXe25LfzhfWx5iySNH': {'name': 'p120', 'twitter': '@p120'},
            '84LuyA7oJbmHjaN7stWefcxxW7FjoawK1UzMmqJBVzGe': {'name': 'p121', 'twitter': '@p121'},
            'Z46NnHq6ZZzC3KY3dWzs6cZq5Jmo6XAEa6LMNT9DAQg': {'name': 'p122', 'twitter': '@p122'},
            'HXkucsKiePNsp16U87EWAYkFnPj3qP7tBNooipExd1f6': {'name': 'p123', 'twitter': '@p123'},
            'GzbVkUZzwYfiHEJ56XM5C5ffx52UomWckywxZiDTC6E3': {'name': 'p124', 'twitter': '@p124'},
            'dSd6inrWwLs5nbxFp7iDMj9zxCoQFbbB2aFRcnvDHaZ': {'name': 'p125', 'twitter': '@p125'},
            'DQY5Gf5Ls2nAjq3BtD3NfzN18Q45inC5TiyB3CCJTTYG': {'name': 'p126', 'twitter': '@p126'},
            'GYzLNpTXbZtcTLp5YNogjMTjFsXsVTgz2sduMZLsYzNS': {'name': 'p127', 'twitter': '@p127'},
            '8FD33dGS13YazoKyYhewbYDKS5ijSE2RaHSMmY5Jv8iY': {'name': 'p128', 'twitter': '@p128'},
            '5vrpoGJTKmWirwzBN5nVsj1Vdujwx3DLpSVWZaE9xb5z': {'name': 'p129', 'twitter': '@p129'},
            '5vxmjUTSydEVev7KpkUbCCrYBBA1XDBrxp6NDVLZisZ': {'name': 'p130', 'twitter': '@p130'},
            '5sbH7EKNXzg6Yatf8fTCMPLg9Aj3pZtwZoaGC4Qdv5LU': {'name': 'p131', 'twitter': '@p131'},
            'FGViyb2UHSoXMALXt4vmgYPizSLYVHxekw7gS6vLUUSm': {'name': 'p132', 'twitter': '@p132'},
            '8Q2R17M9h5FCHz62hWwH3qUowhhsYAyVzNZUp7tvtsP1': {'name': 'p133', 'twitter': '@p133'},
            'CYUU5LtFyLcCh7ZFAxunf6cUxfUPJGquFP2zYRqRitma': {'name': 'p134', 'twitter': '@p134'},
            '3DKtN3TUoyL2VnoRNa8HUncnzodkYPWpktbzPNCEBFUs': {'name': 'p135', 'twitter': '@p135'},
            'CDUAza6NMFDUhjZEaAGHZA8y9AE9bEbm236K5AJhFHRv': {'name': 'p136', 'twitter': '@p136'},
            '5QmqKFFvZfS16a4yYBwZy4ezs7f11Az9A975ENC5xqqj': {'name': 'p137', 'twitter': '@p137'}
        }

        self.wsol_address = "So11111111111111111111111111111111111111112"

    def load_progress(self) -> tuple:
        """Load previous progress if it exists"""
        try:
            with open('kol_progress.json', 'r') as f:
                progress_data = json.load(f)
            
            print(f"Found previous progress from {progress_data['timestamp']}")
            print(f"Completed {progress_data['wallet_index']} wallets")
            
            return progress_data['trades'], progress_data['wallet_index']
        except FileNotFoundError:
            return [], 0

    def save_progress(self, trades: List[Dict], wallet_index: int):
        """Save current progress to a file"""
        progress_data = {
            'wallet_index': wallet_index,
            'trades': trades,
            'timestamp': datetime.now().isoformat()
        }
        
        with open('kol_progress.json', 'w') as f:
            json.dump(progress_data, f, indent=2)

    def get_wallet_transactions(self, wallet: str, hours_back: int = 12) -> List[Dict]:
        """Get transactions for a specific wallet using Helius API"""
        url = f"{self.helius_url}/addresses/{wallet}/transactions"
        
        params = {
            'api-key': self.helius_api_key,
            'limit': 50,
            'parsed': 'true'
        }
        
        max_retries = 3
        for attempt in range(max_retries):  # Add the retry loop
            try:
                # Add timeout to prevent hanging
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 429:
                    wait_time = 2 ** attempt
                    print(f"    Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                    
                response.raise_for_status()
                transactions = response.json()
                
                cutoff_time = datetime.now() - timedelta(hours=hours_back)
                recent_txs = []
                
                for tx in transactions:
                    tx_time = datetime.fromtimestamp(tx['timestamp'])
                    if tx_time >= cutoff_time:
                        recent_txs.append(tx)
                        
                return recent_txs
                
            except requests.exceptions.Timeout:
                print(f"    Timeout on attempt {attempt + 1}")
                if attempt == max_retries - 1:
                    print(f"    Failed after {max_retries} timeout attempts")
                    return []
                time.sleep(2)
                continue
            except requests.exceptions.RequestException as e:
                print(f"    Request error on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    print(f"    Failed after {max_retries} attempts")
                    return []
                time.sleep(2)
                continue

    def parse_swap_transaction(self, tx: Dict, wallet: str) -> List[Dict]:
        """Parse swap transaction to extract token purchases"""
        trades = []
        
        try:
            if 'tokenTransfers' in tx:
                for transfer in tx['tokenTransfers']:
                    if (transfer.get('toUserAccount') == wallet and 
                        transfer.get('mint') != self.wsol_address and
                        transfer.get('mint')):
                        
                        # Simple estimation - replace with real price lookup
                        token_amount = transfer.get('tokenAmount', 0)
                        usd_amount = token_amount * 0.001 if token_amount else 100  # Placeholder
                        
                        if usd_amount > 10:
                            trade = {
                                'wallet': wallet,
                                'token_mint': transfer['mint'],
                                'token_symbol': transfer.get('tokenSymbol', 'Unknown'),
                                'amount_usd': usd_amount,
                                'timestamp': tx['timestamp'],
                                'signature': tx['signature']
                            }
                            trades.append(trade)
                            
        except Exception as e:
            print(f"Error parsing transaction: {e}")
            
        return trades

    def get_all_kol_trades(self, hours_back: int = 12, start_from: int = 0) -> List[Dict]:
        """Get all trades from KOL wallets with resume capability"""
        all_trades = []
        
        print(f"Fetching trades from {len(self.kol_wallets)} KOL wallets...")
        if start_from > 0:
            print(f"Resuming from wallet {start_from + 1}...")
        print("Running with timeouts and retry logic...")
        
        wallets_list = list(self.kol_wallets.items())
        
        for i in range(start_from, len(wallets_list)):
            wallet_address, kol_info = wallets_list[i]
            print(f"Processing {kol_info['name']} [{i+1}/{len(self.kol_wallets)}]...")
            
            try:
                transactions = self.get_wallet_transactions(wallet_address, hours_back)
                
                trade_count = 0
                for tx in transactions:
                    trades = self.parse_swap_transaction(tx, wallet_address)
                    for trade in trades:
                        trade['kol_name'] = kol_info['name']
                        trade['kol_twitter'] = kol_info['twitter']
                        all_trades.append(trade)
                        trade_count += len(trades)
                
                if trade_count > 0:
                    print(f"  Found {trade_count} trades")
                
                # Save progress every 10 wallets
                if (i + 1) % 10 == 0:
                    self.save_progress(all_trades, i + 1)
                    print(f"  Progress saved at wallet {i + 1}")
                
                # .1 second delay
                if i < len(wallets_list) - 1:
                    print(f"  Waiting .1s... ({i+1}/{len(self.kol_wallets)} complete)")
                    time.sleep(.01)
                    
            except KeyboardInterrupt:
                print(f"\nInterrupted! Saving progress at wallet {i + 1}...")
                self.save_progress(all_trades, i + 1)
                print(f"Progress saved. Resume by running the script again.")
                raise
            except Exception as e:
                print(f"  Error processing {kol_info['name']}: {e}")
                continue
                    
        return all_trades

    def aggregate_trades(self, trades: List[Dict]) -> pd.DataFrame:
        """Aggregate trades by token"""
        token_stats = defaultdict(lambda: {
            'token_symbol': '',
            'unique_kols': set(),
            'total_buys': 0,
            'total_volume': 0,
            'kol_names': set()
        })
        
        for trade in trades:
            token_mint = trade['token_mint']
            token_stats[token_mint]['token_symbol'] = trade['token_symbol']
            token_stats[token_mint]['unique_kols'].add(trade['wallet'])
            token_stats[token_mint]['kol_names'].add(trade['kol_name'])
            token_stats[token_mint]['total_buys'] += 1
            token_stats[token_mint]['total_volume'] += trade['amount_usd']
        
        results = []
        for token_mint, stats in token_stats.items():
            results.append({
                'token_symbol': stats['token_symbol'],
                'contract_address': token_mint,
                'unique_kols': len(stats['unique_kols']),
                'total_buys': stats['total_buys'],
                'total_volume': round(stats['total_volume'], 2),
                'kol_names': ', '.join(stats['kol_names']),
            })
        
        df = pd.DataFrame(results)
        return df.sort_values(['unique_kols', 'total_volume'], ascending=[False, False])

    def run_analysis(self, hours_back: int = 12, resume: bool = True) -> pd.DataFrame:
        """Run the complete analysis with resume capability"""
        if resume:
            existing_trades, start_from = self.load_progress()
            if existing_trades:
                user_input = input(f"Resume from wallet {start_from + 1}? (y/n): ").lower()
                if user_input == 'y':
                    new_trades = self.get_all_kol_trades(hours_back, start_from)
                    all_trades = existing_trades + new_trades
                else:
                    all_trades = self.get_all_kol_trades(hours_back, 0)
            else:
                all_trades = self.get_all_kol_trades(hours_back, 0)
        else:
            all_trades = self.get_all_kol_trades(hours_back, 0)
        
        print(f"Found {len(all_trades)} trades from KOLs")
        
        if not all_trades:
            return pd.DataFrame()
        
        results_df = self.aggregate_trades(all_trades)
        
        # Clean up progress file when complete
        try:
            os.remove('kol_progress.json')
            print("Progress file cleaned up")
        except FileNotFoundError:
            pass
            
        return results_df

def main():
    try:
        tracker = KOLTracker()
        results = tracker.run_analysis(hours_back=48)
        
        if not results.empty:
            print("\nTOP KOL PICKS (Last 12 hours):")
            print(results.to_string(index=False))
            
            # Save results
            results.to_csv('kol_tracking_results.csv', index=False)
            print(f"\nResults saved to kol_tracking_results.csv")
            
            # Show consensus picks
            consensus_picks = results[results['unique_kols'] >= 2]
            if not consensus_picks.empty:
                print(f"\nCONSENSUS PICKS ({len(consensus_picks)} tokens with 2+ KOLs):")
                print(consensus_picks[['token_symbol', 'unique_kols', 'total_volume', 'kol_names']].to_string(index=False))
        else:
            print("No trading activity found in the last 12 hours.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()