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

            'CJbtcwcLqzHc4oW63rhfmrAxvL2ZbvgJYDPANGV5YaPt': {'name': 'w1', 'twitter': '@w1'},
            '5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9': {'name': 'w2', 'twitter': '@w2'},
            'BDzu84TVpqEC883KbxSThgBykFV8JFoc2qhfi3KQVp6J': {'name': 'w3', 'twitter': '@w3'},
            'BDFMkurHjWM8HCibLRncxaSF8eQ3oJJztPSr4jUEo3nZ': {'name': 'w4', 'twitter': '@w4'},
            '8i57XsS3E4iuw2qy2cPbKDWnW4pwx6yaBc7N7UQzG3MJ': {'name': 'w5', 'twitter': '@w5'},
            '3eJkwFDZVB27emciij1oWUVodmFhFdnkpmzKHjDzH34o': {'name': 'w6', 'twitter': '@w6'},
            'Ds5jCefMDMCbwjrypDVqUvXn3wqTgHyvwD6iCrJYkWYy': {'name': 'w7', 'twitter': '@w7'},
            '2CR6zpd28GWaQgc8w5hn2gtCbiVzn7ifLhzu4rAmTtDs': {'name': 'w8', 'twitter': '@w8'},
            'E65QjbAxG8ZMoCGt1ChkT8e6MmSeikF2jNZbqnD3ZdiM': {'name': 'w9', 'twitter': '@w9'},
            '7uQxuvVFzY3uApna9PFQEMQEUKrrwFYG4D4272aPHT1d': {'name': 'w10', 'twitter': '@w10'},
            '6Ru8fGXkZCVY1bFoc7dg1Mtt6qyCdESeb9BkRUetqUS7': {'name': 'w11', 'twitter': '@w11'},
            'BbumVEtBca2YVHcPtb8ZNENX9c3mYnPTK9vN62mZ8bnX': {'name': 'w12', 'twitter': '@w12'},
            'Hz3UXqdDtJyV2tk8y7jNvL8bDxYwmSUGpHS4Rg9G1Dk8': {'name': 'w13', 'twitter': '@w13'},
            '4nZ263ccsQj1monDZPMqeHuNaFcitUKhLtWRyGUoGCUZ': {'name': 'w14', 'twitter': '@w14'},
            'BDzury9AA1zKee7gcDnAxruRF9AomgiMLioZYxGnVp6J': {'name': 'w15', 'twitter': '@w15'},
            '2CR6jG5ytiUT1yftSuEz7n1S3mi7oczThN9vA2u6TtDs': {'name': 'w16', 'twitter': '@w16'},
            'HxrAFWBpBFZGWrAxNM2LD66GXjPKcxz8uB6HwtYU3TM7': {'name': 'w17', 'twitter': '@w17'},
            'BMnT51N4iSNhWU5PyFFgWwFvN1jgaiiDr9ZHgnkm3iLJ': {'name': 'w18', 'twitter': '@w18'},
            'EMKbfGCxqtMSjRMPyt7MGWd2zm8WGppvVZAphRUhbzEC': {'name': 'w19', 'twitter': '@w19'},
            'FPDB4tEyq1FTCqu83mBe73nC3GsLM8SNLm8t44EU3oxf': {'name': 'w20', 'twitter': '@w20'},
            'DaJMBi7jdMuXGPWzy4zeERrtpjUKesz9CbnfDerVbY2W': {'name': 'w21', 'twitter': '@w21'},
            '8jqpi2pRyYb9L79ChXp8Zp4HEKEAJidseZqtRKZWfUNf': {'name': 'w22', 'twitter': '@w22'},
            'HQ4JojrjnMegW35Y8fk5xqEUjiuPn6YhKrvjwNM48ZbC': {'name': 'w23', 'twitter': '@w23'},
            '5vTmma3Ay6w3xoC1B1Dstfwtwdvxp8Wmw4Ymqmm2QWAN': {'name': 'w24', 'twitter': '@w24'},
            'FzSFLzPY3Ame3TbeFgr8KAmt4JFhVWFFe9AWPrRHd1rq': {'name': 'w25', 'twitter': '@w25'},
            'Hz73rG8zVjvdCHbKzi2GE3Sx3yDfRQzAb8eRg4YSVCte': {'name': 'w26', 'twitter': '@w26'},
            'GnSC2SNdHbXk8hXWQqYEWgasGb2CY3mpTB3nyXU8DyEN': {'name': 'w27', 'twitter': '@w27'},
            'G6tcDjqi3De3rDjUHS3FvwP3Pvqj2hzwDGhwkGML6vJT': {'name': 'w28', 'twitter': '@w28'},
            'BiESLRE2R32eD2r89v1HQrXAN5gCFNfgr6G1KGiDDX46': {'name': 'w29', 'twitter': '@w29'},
            'CcTNKBhQKYu7nB4eRrRFQoXEvpd6H5QnGKh93pwniBtp': {'name': 'w30', 'twitter': '@w30'},
            '4gQT88rvHr6ay8XvmUUriL5FTjsSuvkvH8ybz3etVLBb': {'name': 'w31', 'twitter': '@w31'},
            '6HRgJMRmjaj2svP9GpRbUU5TPzLAHnBW3sHgYVbirWYE': {'name': 'w32', 'twitter': '@w32'},
            'bKaQgj9UoihD45UXEaKGTodwop4iyKz5NRQcxFGvDCx': {'name': 'w33', 'twitter': '@w33'},
            'ExfKQ3W72wDdrxY1XWduCmmfrxHu3CDRBeRuNAp5qKdS': {'name': 'w34', 'twitter': '@w34'},
            '5XajeMJJF8XCmz4zHETx9T3793driM9CMQUU7KdmnCvA': {'name': 'w35', 'twitter': '@w35'},
            '1NKvBXJBftBvph3m2t2u5hyWWMusUYQ6Wj9KLsGziyE': {'name': 'w36', 'twitter': '@w36'},
            'AvJE5uoCbfFivo4avxfm6LAmyCb2eqEUqDrJQ1bd43np': {'name': 'w37', 'twitter': '@w37'},
            'EQbcCPPYskMdEXMfkPnTwLXn9V92Pok2ZSVdFpvd9tKn': {'name': 'w38', 'twitter': '@w38'}
        }

        """"
        self.kol_wallets = {
            '8deJ9xeUvXSJwicYptA9mHsU2rN2pDx37KWzkDkEXhU6': {'name': 'Cooker', 'twitter': '@CookerFlips'},
            '8zFZHuSRuDpuAR7J6FzwyF3vKNx4CVW3DFHJerQhc7Zd': {'name': 'Pow', 'twitter': '@traderpow'},
            '7VBTpiiEjkwRbRGHJFUz6o5fWuhPFtAmy8JGhNqwHNnn': {'name': 'Brox', 'twitter': '@ohbrox'},
            'HmBmSYwYEgEZuBUYuDs9xofyqBAkw4ywugB1d7R7sTGh': {'name': 'Tobx', 'twitter': '@TobxG'},
            'mW4PZB45isHmnjGkLpJvjKBzVS5NXzTJ8UDyug4gTsM': {'name': 'Dex', 'twitter': '@igndex'},
            'DNfuF1L62WWyW3pNakVkyGGFzVVhj4Yr52jSmdTyeBHm': {'name': 'Gake', 'twitter': '@Ga__ke'},
            'ATKi3ZvMbo31pbgBgGSGQPDPKEbQ4oGzoDrwG2sms56k': {'name': 'Nach', 'twitter': '@NachSOL'},
            '3kebnKw7cPdSkLRfiMEALyZJGZ4wdiSRvmoN4rD1yPzV': {'name': 'Bastille', 'twitter': '@BastilleBtc'},
            'AVAZvHLR2PcWpDf8BXY4rVxNHYRBytycHkcB5z5QNXYm': {'name': 'Ansem', 'twitter': '@blknoiz06'},
            '215nhcAHjQQGgwpQSJQ7zR26etbjjtVdW74NLzwEgQjP': {'name': 'OGAntD', 'twitter': '@0GAntD'},
            '7i7vHEv87bs135DuoJVKe9c7abentawA5ydfWcWc8iY2': {'name': 'ChartFu', 'twitter': '@ChartFuMonkey'},
            'F5TjPySiUJMdvqMZHnPP85Rc1vErDGV5FR5P2vdVm429': {'name': 'Zyaf', 'twitter': '@0xZyaf'},
            '6m5sW6EAPAHncxnzapi1ZVJNRb9RZHQ3Bj7FD84X9rAF': {'name': 'Shocked JS', 'twitter': '@ShockedJS'},
            'DpNVrtA3ERfKzX4F8Pi2CVykdJJjoNxyY5QgoytAwD26': {'name': 'Gorilla Capital', 'twitter': '@gorillacapsol'},
            '7SDs3PjT2mswKQ7Zo4FTucn9gJdtuW4jaacPA65BseHS': {'name': 'Insentos', 'twitter': '@insentos'},
            'ApRnQN2HkbCn7W2WWiT2FEKvuKJp9LugRyAE1a9Hdz1': {'name': 'S', 'twitter': '@runitbackghost'},
            '9AqzsYXj1M2z8shG6resmM7LNM6GdvsjcjhjRUPc1dNf': {'name': 'Cuh', 'twitter': '@quarsays'},
            'GwoFJFjUTUSWq2EwTz4P2Sznoq9XYLrf8t4q5kbTgZ1R': {'name': 'Levis', 'twitter': '@LevisNFT'},
            'DeVjHYTEZEi7Wvcvfjz8KZMzpuZpijABgutSfXn1BxjX': {'name': 'Duke', 'twitter': '@dukezfn'},
            '41uh7g1DxYaYXdtjBiYCHcgBniV9Wx57b7HU7RXmx1Gg': {'name': 'Lowskii', 'twitter': '@Lowskii_gg'},
            'c3XGUoDSBaJDA8qaJ5pUkCnamMERwZLJBVjxdkNepGo': {'name': 'Mog', 'twitter': '@10piecedawg'},
            'FRbUNvGxYNC1eFngpn7AD3f14aKKTJVC6zSMtvj2dyCS': {'name': 'Henn100x', 'twitter': '@henn100x'},
            '8MaVa9kdt3NW4Q5HyNAm1X5LbR8PQRVDc1W8NMVK88D5': {'name': 'Daumen', 'twitter': '@daumeneth'},
            'm7Kaas3Kd8FHLnCioSjCoSuVDReZ6FDNBVM6HTNYuF7': {'name': 'Ferb', 'twitter': '@ferbsol'},
            'HwRnKq7RPtKHvX9wyHsc1zvfHtGjPQa5tyZtGtbvfXE': {'name': 'Jay', 'twitter': '@BitBoyJay'},
            'FbvUU5qvD9JsU9jp3KDweCpZiVZHLoQBQ1PPCAAbd6FB': {'name': 'Profitier', 'twitter': '@profitierr'},
            'Aen6LKc7sGVPTyjMd5cu9B9XVjL7m9pnvAiP2ZNJC4GZ': {'name': 'Aroa', 'twitter': '@AroaOnSol'},
            '3h65MmPZksoKKyEpEjnWU2Yk2iYT5oZDNitGy5cTaxoE': {'name': 'Jidn', 'twitter': '@jidn_w'},
            'AE3tJDEyUdwBM8ZoUb3iCo563gMbq26JtckfvjcVZbSa': {'name': 'Roxo', 'twitter': '@RoxoXBT'},
            '5AyJw1VNDgTho2chipbVmuGqTuX1fCvVkLneChQkQrw8': {'name': 'Bolivian', 'twitter': '@_bolivian'},
            'Ds8mcuP5r2phg596mLui3ti3PJtVvFRw19Eo9UFdJ5Bc': {'name': 'Jazz', 'twitter': '@youngjazzeth'},
            '2BU3NAzgRA2gg2MpzwwXpA8X4CCRaLgrf6TY1FKfJPX2': {'name': 'Issa', 'twitter': '@issathecooker'},
            '9vWutdTBs66hWkeCmxaLFpkKy4q5RSe8DsFjfdxj5yFA': {'name': 'Dutch', 'twitter': '@0xDutch_'},
            '5wcc13mXoyqe6qh2iHH5GFknojoJ7y13ZPx9K4NXTuo3': {'name': 'Dolo', 'twitter': '@doloxbt'},
            'GM7Hrz2bDq33ezMtL6KGidSWZXMWgZ6qBuugkb5H8NvN': {'name': 'Beaver', 'twitter': '@beaverd'},
            '6S8GezkxYUfZy9JPtYnanbcZTMB87Wjt1qx3c6ELajKC': {'name': 'Nyhrox', 'twitter': '@nyhrox'},
            '4EsY8HQB4Ak65diFrSHjwWhKSGC8sKmnzyusM993gk2w': {'name': 'Prosciutto', 'twitter': '@prosciuttosol'},
            'B7FDzJnb7DLYH1A37tKEFss1RRSz5C7MYgTRMaWzBqa': {'name': 'Lucas', 'twitter': '@LockedInLucas'},
            '9K18MstUaXmSFSBoa9qDTqWTnYhTZqdgEhuKRTVRgh6g': {'name': 'Sabby', 'twitter': '@sabby_eth'},
            '4AHgEkTsGqY77qtde4UJn9yZCrbGcM7UM3vjT3qM4G5H': {'name': 'BagCalls', 'twitter': '@BagCalls'},
            'D2wBctC1K2mEtA17i8ZfdEubkiksiAH2j8F7ri3ec71V': {'name': 'Dior', 'twitter': '@Dior100x'},
            '42nsEk51owYM3uciuRvFerqK77yhXZyjBLRgkDzJPV2g': {'name': 'Izzy', 'twitter': '@degenIzzy'},
            '6Qs6joB349h7zu1z9xRgPgMSmpBYLDQb2wtAecY4LysH': {'name': 'Chefin', 'twitter': '@Chefin100x'},
            'HCQb4Qrtk4qCfChDH8XvM5onymmCSmRj9bddV6QCPdRe': {'name': 'Rektober', 'twitter': '@rektober'},
            'iWinRYGEWcaFFqWfgjh28jnqWL72XUMmUfhADpTQaRL': {'name': 'Imperator', 'twitter': '@imperooterxbt'},
            'HUS9ErdrDqpqQePbmfgJUTnDTE6eZ8ES62a25RihSK9U': {'name': 'Hustler', 'twitter': '@JoeVargas'},
            '525LueqAyZJueCoiisfWy6nyh4MTvmF4X9jSqi6efXJT': {'name': 'Joji', 'twitter': '@metaversejoji'},
            '7SvvTJSwcpNQCDGEt14zuoPFoSsaveGd7UWgiJGQLAHN': {'name': 'Mero', 'twitter': '@MeroFN'},
            '3FDci33mzMKNdNxzSS9D13XyNZQAdfmpvtDZLWPbZiAU': {'name': 'Sachs', 'twitter': '@gudmansachs'},
            '9Vk7pkBZ9KFJmzaPzNYjGedyz8qoKMQtnYyYi2AehNMT': {'name': 'Xelf', 'twitter': '@xelf_sol'},
            '8YYDiCbPd4nM8TxrQEVdPA4aG8jys8R7Z1kKsgPL4pwh': {'name': 'Michi', 'twitter': '@michigems'},
            '4vw54BmAogeRV3vPKWyFet5yf8DTLcREzdSzx4rw9Ud9': {'name': 'Decu', 'twitter': '@Decu0x'},
            '4jFPYSoUTRaFbFDJp9QpA1J5xJmMJYoWhiFTpoLPq6X': {'name': 'Merk', 'twitter': '@MerkTrading'},
            '9yGxZ43ngT7LvwquVdUAYPvJzVyY65cS6mQvuJXjTEUc': {'name': 'Xet', 'twitter': '@xet'},
            '4F2AHuw55m9ojKpFfsofmhAwB979ECVRFVurEam4phqU': {'name': 'Lunix', 'twitter': '@SolLunix'},
            'D6JrzpAKtLT4XQmgVKxdKZwFxJ4HmZWVKmMfKEK4av6d': {'name': 'Solana Plays', 'twitter': '@SolanaPlays'},
            'DBmRHNbSsVX8F6NyVaaaiuGdwo1aYGawiy3jfNcvXYSC': {'name': 'Bobby', 'twitter': '@retardmode'},
            '3mPypxb7ViYEdLv4siFmESvY5w5ZKknwgmB4TPcZ77qe': {'name': 'Carti The Menace', 'twitter': '@CartiTheMenace'},
            'J2B5fnm2DAAUAGa4EaegwQFoYaN6B5FerGA5sjtQoaGM': {'name': 'Dan176', 'twitter': '@176Dan'},
            'B3beyovNKBo4wF1uFrfGfpVeyEHjFyJPmEBciz7kpnoS': {'name': 'CC2', 'twitter': '@CC2Ventures'},
            'EAnB5151L8ejp3SM6haLgyv3snk6oqc8acKgWEg9T5J': {'name': 'Frosty', 'twitter': '@ohFrostyyy'},
            'ExKCuoAzJCgCVjU3CvNoL8vVrdESTWkx3ubj6rQXwQM4': {'name': 'TheDefiApe', 'twitter': '@TheDefiApe'},
            'C4BWYccLsbeHgZzVupFZJGvJK2nQpn8em9WtzURH4gZW': {'name': 'Obijai', 'twitter': '@Obijai'},
            'FEYmfSL4B3Hhk5BzyEJNpHRiwAoMvzdNe2oa1UTKmbYT': {'name': 'Idontpaytaxes', 'twitter': '@untaxxable'},
            'HrCPnDvDgbpbFxKxer6Pw3qEcfAQQNNjb6aJNFWgTEng': {'name': '0xWinged', 'twitter': '@0xExorcized'},
            'Ebk5ATdfrCuKi27dHZaw5YYsfREEzvvU8xeBhMxQoex6': {'name': 'Sully', 'twitter': '@sullyfromDeets'},
            'AGnd5WTHMUbyK3kjjQPdQFM3TbWcuPTtkwBFWVUwiCLu': {'name': 'Angi', 'twitter': '@angitradez'},
            'CSHktdVEmJybwNR9ft3sDfSc2UKgTPZZ8km26XfHYZDt': {'name': 'Lynk', 'twitter': '@lynk0x'},
            '86AEJExyjeNNgcp7GrAvCXTDicf5aGWgoERbXFiG1EdD': {'name': 'Publix', 'twitter': '@publixplays'},
            '3nvC8cSrEBqFEXZjUpKfwZMPk7xYdqcnoxmFBjXiizVX': {'name': 'Value & Time', 'twitter': '@valueandtime'},
            'H9h6UV9cwWWogcTZ5gbzQ6Z3dYDNaiJ3mUWw3aGnE3Mc': {'name': 'Monarky', 'twitter': '@MonarkyMemes'},
            'HLv6yCEpgjQV9PcKsvJpem8ESyULTyh9HjHn9CtqSek1': {'name': 'Lyxe', 'twitter': '@cryptolyxe'},
            'CqQKv6XdrMWrz3YuSwqTTcVoQK5eu4zNo3hps3M1Q3yo': {'name': 'Jaden', 'twitter': '@JadenOnChain'},
            'DsqRyTUh1R37asYcVf1KdX4CNnz5DKEFmnXvgT4NfTPE': {'name': 'Classic', 'twitter': '@mrclassic33'},
            'Gv7CnRo2L2SJ583XEfoKHKbmWK3wNoBDxVoJqMKJR4Nu': {'name': 'Robo', 'twitter': '@roboPBOC'},
            '7ABz8qEFZTHPkovMDsmQkm64DZWN5wRtU7LEtD2ShkQ6': {'name': 'Red', 'twitter': '@redwithbag'},
            '4Be9CvxqHW6BYiRAxW9Q3xu1ycTMWaL5z8NX4HR3ha7t': {'name': 'Mitch', 'twitter': '@1solinfeb'},
            'EdDCRfDDeiiDXdntrP59abH4DXHFNU48zpMPYisDMjA7': {'name': 'Mezoteric', 'twitter': '@mezoteric'},
            'DYAn4XpAkN5mhiXkRB7dGq4Jadnx6XYgu8L5b3WGhbrt': {'name': 'The Doc', 'twitter': '@KayTheDoc'},
            'F2SuErm4MviWJ2HzKXk2nuzBC6xe883CFWUDCPz6cyWm': {'name': 'Earl', 'twitter': '@earlTrades'},
            '2kv8X2a9bxnBM8NKLc6BBTX2z13GFNRL4oRotMUJRva9': {'name': 'Gh0stee', 'twitter': '@4GH0STEE'},
            '5B52w1ZW9tuwUduueP5J7HXz5AcGfruGoX6YoAudvyxG': {'name': 'Yenni', 'twitter': '@Yennii56'},
            '831yhv67QpKqLBJjbmw2xoDUeeFHGUx8RnuRj9imeoEs': {'name': 'Trey', 'twitter': '@treysocial'},
            '4DdrfiDHpmx55i4SPssxVzS9ZaKLb8qr45NKY9Er9nNh': {'name': 'Mr. Frog', 'twitter': '@TheMisterFrog'},
            'AbcX4XBm7DJ3i9p29i6sU8WLmiW4FWY5tiwB9D6UBbcE': {'name': '404Flipped', 'twitter': '@404flipped'},
            '34ZEH778zL8ctkLwxxERLX5ZnUu6MuFyX9CWrs8kucMw': {'name': 'Groovy', 'twitter': '@0xGroovy'},
            '5x8tfrJSn4Pt5gjQEMWDnoLvAzZ8rgJVTXiTpcwhbxmN': {'name': 'Tahi', 'twitter': '@Tahifn'},
            '7tiRXPM4wwBMRMYzmywRAE6jveS3gDbNyxgRrEoU6RLA': {'name': 'Qtdegen', 'twitter': '@qtdegen'},
            '2CXbN6nuTTb4vCrtYM89SfQHMMKGPAW4mvFe6Ht4Yo6z': {'name': 'MoneyMaykah', 'twitter': '@moneymaykah_'},
            '7N4bAyZX6z39RozQh7GC8VQmgjvjkkWmSyAt1wdMjEmq': {'name': 'Zinc', 'twitter': '@Zinc_ETH'},
            'E33jP6RWVpGkv3fDVbuR5Ee6ak42tTKW9yYszqERtobs': {'name': 'Jerry', 'twitter': '@chefjerrry'},
            '9yYya3F5EJoLnBNKW6z4bZvyQytMXzDcpU5D6yYr4jqL': {'name': 'Loopierr', 'twitter': '@Loopierr'},
            '9FNz4MjPUmnJqTf6yEDbL1D4SsHVh7uA8zRHhR5K138r': {'name': 'Danny', 'twitter': '@0xSevere'},
            '2YJbcB9G8wePrpVBcT31o8JEed6L3abgyCjt5qkJMymV': {'name': 'Al4n', 'twitter': '@Al4neu'},
            '831qmkeGhfL8YpcXuhrug6nHj1YdK3aXMDQUCo85Auh1': {'name': 'Meechie', 'twitter': '@973Meech'},
            '6LChaYRYtEYjLEHhzo4HdEmgNwu2aia8CM8VhR9wn6n7': {'name': 'Assasin.eth', 'twitter': '@assasin_eth'},
            '8rvAsDKeAcEjEkiZMug9k8v1y8mW6gQQiMobd89Uy7qR': {'name': 'Casino', 'twitter': '@casino616'},
            'GJA1HEbxGnqBhBifH9uQauzXSB53to5rhDrzmKxhSU65': {'name': 'Latuche', 'twitter': '@Latuche95'},
            'BXNiM7pqt9Ld3b2Hc8iT3mA5bSwoe9CRrtkSUs15SLWN': {'name': 'Absol', 'twitter': '@absolquant'},
            '4WPTQA7BB4iRdrPhgNpJihGcxKh8T43gLjMn5PbEVfQw': {'name': 'Oura', 'twitter': '@Oura456'},
            '7iabBMwmSvS4CFPcjW2XYZY53bUCHzXjCFEFhxeYP4CY': {'name': 'Leens', 'twitter': '@leensx100'},
            '5TuiERc4X7EgZTxNmj8PHgzUAfNHZRLYHKp4DuiWevXv': {'name': 'Rev', 'twitter': '@solrevv'},
            'BCagckXeMChUKrHEd6fKFA1uiWDtcmCXMsqaheLiUPJd': {'name': 'dv', 'twitter': '@vibed333'},
            'BTf4A2exGK9BCVDNzy65b9dUzXgMqB4weVkvTMFQsadd': {'name': 'Kev', 'twitter': '@Kevsznx'},
            '96sErVjEN7LNJ6Uvj63bdRWZxNuBngj56fnT9biHLKBf': {'name': 'Orange', 'twitter': '@OrangeSBS'},
            '4S9U8HckRngscHWrW418cG6Suw62dhEZzmyrT2hxSye5': {'name': 'Polar', 'twitter': '@polarsterrr'},
            'EHg5YkU2SZBTvuT87rUsvxArGp3HLeye1fXaSDfuMyaf': {'name': 'TIL', 'twitter': '@tilcrypto'},
            'EaVboaPxFCYanjoNWdkxTbPvt57nhXGu5i6m9m6ZS2kK': {'name': 'Danny', 'twitter': '@cladzsol'},
            'F72vY99ihQsYwqEDCfz7igKXA5me6vN2zqVsVUTpw6qL': {'name': 'Jalen', 'twitter': '@RipJalens'},
            '4BdKaxN8G6ka4GYtQQWk4G4dZRUTX2vQH9GcXdBREFUk': {'name': 'Jijo', 'twitter': '@jijo_exe'},
            '5rkPDK4JnVAumgzeV2Zu8vjggMTtHdDtrsd5o9dhGZHD': {'name': 'Dave Portnoy', 'twitter': '@stoolpresidente'},
            'BCnqsPEtA1TkgednYEebRpkmwFRJDCjMQcKZMMtEdArc': {'name': 'Kreo', 'twitter': '@kreo444'},
            'DfMxre4cKmvogbLrPigxmibVTTQDuzjdXojWzjCXXhzj': {'name': 'Euris', 'twitter': '@Euris_JT'},
            'CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o': {'name': 'Cented', 'twitter': '@Cented7'},
            'CRVidEDtEUTYZisCxBZkpELzhQc9eauMLR3FWg74tReL': {'name': 'Frank', 'twitter': '@frankdegods'},
            'AJ6MGExeK7FXmeKkKPmALjcdXVStXYokYNv9uVfDRtvo': {'name': 'Tim', 'twitter': '@timmpix1'},
            '73LnJ7G9ffBDjEBGgJDdgvLUhD5APLonKrNiHsKDCw5B': {'name': 'Waddles', 'twitter': '@waddles_eth'},
            'JDd3hy3gQn2V982mi1zqhNqUw1GfV2UL6g76STojCJPN': {'name': 'West', 'twitter': '@ratwizardx'},
            '3LYtEmYerFPeXgu1c9Y4553oMk2qVdcxiwWzDjzMkwPx': {'name': 'awkchan45', 'twitter': '@awkchan45'},
            'AeLaMjzxErZt4drbWVWvcxpVyo8p94xu5vrg41eZPFe3': {'name': 's1mple_s1mple', 'twitter': '@s1mple_s1mple'},
            'UxuuMeyX2pZPHmGZ2w3Q8MysvExCAquMtvEfqp2etvm': {'name': 'pandoraflips', 'twitter': '@pandoraflips'},
            'FAicXNV5FVqtfbpn4Zccs71XcfGeyxBSGbqLDyDJZjke': {'name': 'radiance', 'twitter': '@radiancebrr'}
        }

        """
        




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
                        usd_amount = token_amount * 0.01 if token_amount else 100  # Placeholder
                        
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
                    time.sleep(.1)
                    
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
        results = tracker.run_analysis(hours_back=12)
        
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