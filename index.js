require("dotenv").config();
const {
  Client,
  Intents,
  EmbedBuilder
} = require("discord.js");
const clientNewListings = new Client({
  intents: [
  ],
});
clientNewListings.login(process.env.BOT_TOKEN_LISTINGS);
const redis = require("redis");
var fs = require("fs");
const axios = require("axios");
const SYNTH_USD_MAINNET = "0x57ab1ec28d129707052df4df418d58a2d46d5f51";
const Web3 = require("web3");
const web3Arbitrum = new Web3(new Web3.providers.HttpProvider("https://arbitrum-mainnet.infura.io/v3/71f890a2441d49088e4e145b2bc23bc7"));

const web3ArbitrumV2 = new Web3(new Web3.providers.HttpProvider(process.env.INFURA_ARB_URL));

let arbitrumRaw = fs.readFileSync('contracts/arbitrum.json');
let arbitrumContract = JSON.parse(arbitrumRaw);
const web3 = new Web3(new Web3.providers.HttpProvider(process.env.INFURA_URL));
const web3L2 = new Web3(new Web3.providers.HttpProvider(process.env.INFURA_L2_URL));
const web3Base = new Web3(new Web3.providers.HttpProvider("https://mainnet.base.org"));
const CoinGecko = require('coingecko-api');
const CoinGeckoClient = new CoinGecko();
const oddslib = require('oddslib');


let contractV2OTRaw = fs.readFileSync('contracts/v2overtime.json');
let v2ContractRaw = JSON.parse(contractV2OTRaw);
let v2Contract = new web3L2.eth.Contract(v2ContractRaw,"0x2367FB44C4C2c4E5aAC62d78A55876E01F251605");
let FREE_BET_ARB = "0xd1F2b87a9521315337855A132e5721cfe272BBd9";
let STAKING_ARB = "0x109e966A4d856B82f158BF528395de6fF36214A8";
let overtimeV2TradesKey = "overtimeV2TradesKey";
let overtimeV2ARBTradesKey = "overtimeV2ARBTradesKey";
let FREE_BET_OP = "0x8D18e68563d53be97c2ED791CA4354911F16A54B";
let STAKING_OP = "0x5e6D44B17bc989652920197790eF626b8a84e219";

let contractStakingTRaw = fs.readFileSync('contracts/staking.json');
let v2StakingRaw = JSON.parse(contractStakingTRaw);

let v2StakingARB = new web3Arbitrum.eth.Contract(v2StakingRaw,STAKING_ARB);
let v2StakingOP = new web3L2.eth.Contract(v2StakingRaw,STAKING_OP);

let v2FreeBetARB = new web3Arbitrum.eth.Contract(v2StakingRaw,FREE_BET_ARB);
let v2FreeBetgOP = new web3L2.eth.Contract(v2StakingRaw,FREE_BET_OP);


let contractV2TicketRaw = fs.readFileSync('contracts/v2overtimeTicket.json');
let v2ContractTicketRaw = JSON.parse(contractV2TicketRaw);
let v2TicketContract = new web3L2.eth.Contract(v2ContractTicketRaw,"0x71CE219942FFD9C1d8B67d6C35C39Ae04C4F647B");

let v2ARBContract = new web3ArbitrumV2.eth.Contract(v2ContractRaw,"0xB155685132eEd3cD848d220e25a9607DD8871D38");

let v2ARBTicketContract = new web3ArbitrumV2.eth.Contract(v2ContractTicketRaw,"0x04386f9b2b4f713984Fe0425E46a376201641649");

let writenOvertimeV2Trades = [];
let writenOvertimeV2ARBTrades = [];

if (process.env.REDIS_URL) {
  redisClient = redis.createClient(process.env.REDIS_URL);
  redisClient.on("error", function (error) {
    console.error(error);
  });

  redisClient.lrange(overtimeV2TradesKey, 0, -1, function (err, polygonTrades) {
    writenOvertimeV2Trades = polygonTrades;
  });

  redisClient.lrange(overtimeV2ARBTradesKey, 0, -1, function (err, polygonTrades) {
    writenOvertimeV2ARBTrades = polygonTrades;
  });

}

function  formatV2Amount(numberForFormating, collateralAddress) {

  if(collateralAddress == "0x4200000000000000000000000000000000000006" || collateralAddress == "0x217D47011b23BB961eB6D93cA9945B7501a5BB11") {
    return numberForFormating / 1e18;
  } else {
    return numberForFormating / 1e6
  }
}

function  formatV2ARBAmount(numberForFormating, collateralAddress) {

  if(collateralAddress == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831") {
    return numberForFormating / 1e6;
  } else {
    return numberForFormating / 1e18
  }
}



function roundTo2Decimals(number){
  if(1-Math.floor(Math.log(number)/Math.log(10)>0))
    return  number.toFixed(1-Math.floor(Math.log(number)/Math.log(10)))
  else return number.toFixed(2);
}

let WINNER = new Array;
WINNER.push(0, 10011, 10012,
    1001,
    10021,
    10022,
    10023,
    10024,
    10025,
    10026,
    10027,
    10028,
    10029,
    10051,
    10052,
    10053,
    10054,
    10055,
    10056,
    10057,
    10058,
    10059,
    10019,
    10020,
    10130
);

let HANDICAP = new Array;
HANDICAP.push(10001,
    10013,
    10041,
    10042,
    10043,
    10044,
    10045,
    10046,
    10047,
    10048,
    10049,
    10071,
    10072,
    10073,
    10074,
    10075,
    10076,
    10077,
    10078,
    10079
);

let WINNING_ROUND = new Array;
WINNING_ROUND.push(10151);

let ENDING_METHOD = new Array;
ENDING_METHOD.push(10157);
let METHOD_OF_VICTORY = new Array;
METHOD_OF_VICTORY.push(10158);

let TOTAL_HOME_FIRST = new Array;
TOTAL_HOME_FIRST.push(10111)

    let TOTAL_HOME_SECOND = new Array;
TOTAL_HOME_FIRST.push(10211, 10017)

    let TOTAL_AWAY_SECOND = new Array;
TOTAL_AWAY_SECOND.push(10212,10018)

    let TOTAL_AWAY_FIRST = new Array;
TOTAL_AWAY_FIRST.push(10112)

let TOTAL = new Array;
TOTAL.push(10002,
    10007,
    11203,
    10014,
    10031,
    10032,
    10033,
    10034,
    10035,
    10036,
    10037,
    10038,
    10039,
    10061,
    10062,
    10063,
    10064,
    10065,
    10066,
    10067,
    10068,
    10069,
    11010,
    11012,
    11019,
    11029,
    11035,
    11038,
    11039,
    11047,
    11051,
    11052,
    11053,
    11055,
    11056,
    11057,
    11058,
    11060,
    11086,
    11097,
    11098,
    11011,
    11200,
    11011,
    11200,
    11201,
    11202,
    11204,
    11205,
    11206,
    11207,
    11208,
    11209,
    11210,
    11211,
    11212,
    11213,
    11214,
    11215,
    11216,
    11217,
    11218,
    11219,
    11220,
    11221,
    11222,
    11223,
    11225,
    11226,
    11227,
    11228,
    11229,
    11230);

let W_TOTAL = new Array;
W_TOTAL.push(10004);
let HT_TOTAL = new Array;
HT_TOTAL.push(10008);

let DOUBLE_CHANCE = new Array;
DOUBLE_CHANCE.push(10003,
    10015,
    10016);
let HALF_END = new Array;
HALF_END.push(10006);
let BINARY = new Array;
BINARY.push(10009,
    10081,
    10082,
    10083,
    10084,
    10085,
    10086,
    10087,
    10088,
    10089,
    10091,
    10092,
    10093,
    10094,
    10095,
    10096,
    10097,
    10098,
    10099,
    10101,
    10102,
    10103,
    10104,
    10105,
    10106,
    10107,
    10108,
    10109);
let DRAW_NO_BET = new Array;
DRAW_NO_BET.push(10010,
    10121,
    10122,
    10123,
    10124);

let CORRECT_SCORE_LIST = new Array;
CORRECT_SCORE_LIST.push(10100,10200,10201)

let CORRECT_SCORE_MAP = new Map;
CORRECT_SCORE_MAP.set("0","0:0");
CORRECT_SCORE_MAP.set("1","1:1");
CORRECT_SCORE_MAP.set("2","2:2");
CORRECT_SCORE_MAP.set("3","3:3");
CORRECT_SCORE_MAP.set("4","4:4");
CORRECT_SCORE_MAP.set("5","1:0");
CORRECT_SCORE_MAP.set("6","2:0");
CORRECT_SCORE_MAP.set("7","2:1");
CORRECT_SCORE_MAP.set("8","3:0");
CORRECT_SCORE_MAP.set("9","3:1");
CORRECT_SCORE_MAP.set("10","3:2");
CORRECT_SCORE_MAP.set("11","4:0");
CORRECT_SCORE_MAP.set("12","4:1");
CORRECT_SCORE_MAP.set("13","4:2");
CORRECT_SCORE_MAP.set("14","4:3");
CORRECT_SCORE_MAP.set("15","0:1");
CORRECT_SCORE_MAP.set("16","0:2");
CORRECT_SCORE_MAP.set("17","1:2");
CORRECT_SCORE_MAP.set("18","0:3");
CORRECT_SCORE_MAP.set("19","1:3");
CORRECT_SCORE_MAP.set("20","2:3");
CORRECT_SCORE_MAP.set("21","0:4");
CORRECT_SCORE_MAP.set("22","1:4");
CORRECT_SCORE_MAP.set("23","2:4");
CORRECT_SCORE_MAP.set("24","3:4");
CORRECT_SCORE_MAP.set("25","score not mapped");

clientNewListings.once("ready", () => {
  console.log("initial new operations");
  updateTokenPrice();
});

setInterval(function () {
  console.log("get L2 trades");
  getOvertimeV2Trades();
}, 5 * 60 * 1000);

setInterval(function () {
  console.log("get L2 trades");
  getOvertimeV2ARBTrades();
}, 7 * 60 * 1000);

function  isTenisV2(sportId) {
  if(sportId.startsWith("153") || sportId.startsWith("156")) {
    return true;
  } else {
    return false;
  }
}

async function getV2MessageContent(overtimeMarketTrade,typeMap) {
  let homeTeam;
  let awayTeam;
  let position = overtimeMarketTrade.marketsData[0].position;
  let specificGame = await axios.get('https://api.thalesmarket.io/overtime-v2/games-info/' + overtimeMarketTrade.marketsData[0].gameId);
  specificGame = specificGame.data;
  if (specificGame.teams[0].isHome) {
    homeTeam = specificGame.teams[0].name;
    awayTeam = specificGame.teams[1].name;
  } else {
    awayTeam = specificGame.teams[0].name;
    homeTeam = specificGame.teams[1].name
  }
  let marketType = typeMap.get(overtimeMarketTrade.marketsData[0].typeId).name;
  let marketMessage;
  if (overtimeMarketTrade.marketsData[0].playerId && overtimeMarketTrade.marketsData[0].playerId > 0) {
    let specificPlayer = await axios.get('https://api.thalesmarket.io/overtime-v2/players-info/' + overtimeMarketTrade.marketsData[0].playerId);
    specificPlayer = specificPlayer.data.playerName;
    marketMessage = specificPlayer;
  } else {
    marketMessage = homeTeam + " - " + awayTeam;
  }

  let marketId = typeMap.get(overtimeMarketTrade.marketsData[0].typeId).id;
  let betMessage;
  if (WINNER.includes(marketId)) {
    if (position == 0)
      betMessage = homeTeam;
    else if (position == 1) {
      betMessage = awayTeam;
    } else {
      betMessage = "Draw";
    }
  }
  else if(WINNING_ROUND.includes(marketId)){
    switch (position) {
      case 0:
        betMessage = "Draw";
      case 1:
        betMessage = "By decision";
      default: {
        let round = position -1;
        betMessage = "Round: "+round;
      }
    }
  }
  else if(ENDING_METHOD.includes(position)){
    switch (position) {
      case 0:
        betMessage = "Draw";
      case 1:
        betMessage = "By decision";
      case 2:
        betMessage = "KO/TKO/DQ";
      case 3:
        betMessage = "Submission";
      default: betMessage = "";
    }
  }
  else if(METHOD_OF_VICTORY.includes(marketId)){
    switch (position) {
      case 0:
        return 'Draw';
      case 1:
        return homeTeam+" (Decision)";
      case 2:
        return homeTeam+" (KO/TKO/DQ)";
      case 3:
        return homeTeam+" (Submission)";
      case 4:
        return awayTeam+" (Decision)";
      case 5:
        return awayTeam+" (KO/TKO/DQ)";
      case 6:
        return awayTeam+" (Submission)";
      default:
        return '';
    }
  } else if(CORRECT_SCORE_LIST.includes(marketId)){
    betMessage =  CORRECT_SCORE_MAP.get(position);
  }
  else if (HANDICAP.includes(marketId)) {
    if (isTenisV2(overtimeMarketTrade.marketsData[0].sportId)) {
      if (position == 0) {
        if (overtimeMarketTrade.marketsData[0].typeId == "10001") {
          betMessage = "H1(" + overtimeMarketTrade.marketsData[0].line / 100 + ") games"
        } else {
          betMessage = "H1(" + overtimeMarketTrade.marketsData[0].line / 100 + ") sets"
        }
      } else {
        if (overtimeMarketTrade.marketsData[0].typeId == "10001") {
          betMessage = "H2(" + overtimeMarketTrade.marketsData[0].line / 100 + ") games"
        } else {
          betMessage = "H2(" + overtimeMarketTrade.marketsData[0].line / 100 + ") sets"
        }
      }
    } else {
      if (position == 0) {
        betMessage = "H1(" + overtimeMarketTrade.marketsData[0].line / 100 + ")";
      } else {
        betMessage = "H2(" + overtimeMarketTrade.marketsData[0].line / 100 + ")";
      }
    }
  } else if (TOTAL.includes(marketId)) {
    if (isTenisV2(overtimeMarketTrade.marketsData[0].sportId)) {
      if (position == 0)
        if (overtimeMarketTrade.marketsData[0].typeId == "10014") {
          betMessage = "O(" + overtimeMarketTrade.marketsData[0].line / 100 + ") sets";
        } else {
          betMessage = "O(" + overtimeMarketTrade.marketsData[0].line / 100 + ") games"
        }
      else {
        if (overtimeMarketTrade.marketsData[0].typeId == "10014") {
          betMessage = "U(" + overtimeMarketTrade.marketsData[0].line / 100 + ") sets";
        } else {
          betMessage = "U(" + overtimeMarketTrade.marketsData[0].line / 100 + ") games";
        }
      }

    } else {
      if (position == 0)
        betMessage = "O(" + overtimeMarketTrade.marketsData[0].line / 100 + ")";
      else {
        betMessage = "U(" + overtimeMarketTrade.marketsData[0].line / 100 + ")";
      }
    }
  } else if (TOTAL_HOME_FIRST.includes(marketId)) {
    if (position == 0)
      betMessage = homeTeam + " O(" + overtimeMarketTrade.marketsData[0].line / 100 + ")";
    else {
      betMessage = homeTeam + " U(" + overtimeMarketTrade.marketsData[0].line / 100 + ")";
      ;
    }
  } else if (TOTAL_HOME_SECOND.includes(marketId)) {
    if (position == 0)
      betMessage = homeTeam + " O(" + overtimeMarketTrade.marketsData[0].line / 100 + ")";
    else {
      betMessage = homeTeam + " U(" + overtimeMarketTrade.marketsData[0].line / 100 + ")";
      ;
    }
  } else if (TOTAL_AWAY_SECOND.includes(marketId)) {
    if (position == 0)
      betMessage = awayTeam + " O(" + overtimeMarketTrade.marketsData[0].line / 100 + ")";
    else {
      betMessage = awayTeam + " U(" + overtimeMarketTrade.marketsData[0].line / 100 + ")";
      ;
    }
  } else if (TOTAL_AWAY_FIRST.includes(marketId)) {
    if (position == 0)
      betMessage = awayTeam + " O(" + overtimeMarketTrade.marketsData[0].line / 100 + ")";
    else {
      betMessage = awayTeam + " U(" + overtimeMarketTrade.marketsData[0].line / 100 + ")";
      ;
    }
  } else if (DRAW_NO_BET.includes(marketId)) {
    if (position == 0)
      betMessage = homeTeam;
    else {
      betMessage = awayTeam;
    }
  } else if (DOUBLE_CHANCE.includes(marketId)) {
    if (position == 0)
      betMessage = "1X";
    else if (position == 2) {
      betMessage = "X2";
    } else {
      betMessage = "1 or 2";
    }
  } else if (HALF_END.includes(marketId)) {

    let position1 = overtimeMarketTrade.marketsData[0].combinedPositions[0].position;
    let position2 = overtimeMarketTrade.marketsData[0].combinedPositions[1].position;

    if (position1 == 0)
      betMessage = homeTeam;
    else if (position1 == 1) {
      betMessage = awayTeam;
    } else {
      betMessage = "Draw";
    }
    if (position2 == 0)
      betMessage = betMessage + "/" + homeTeam;
    else if (position2 == 1) {
      betMessage = betMessage + "/" + awayTeam;
    } else {
      betMessage = betMessage + "/" + "Draw";
    }
  } else if (W_TOTAL.includes(marketId)) {

    let position1 = overtimeMarketTrade.marketsData[0].combinedPositions[0].position;
    let position2 = overtimeMarketTrade.marketsData[0].combinedPositions[1].position;

    if (position1 == 0)
      betMessage = homeTeam;
    else if (position1 == 1) {
      betMessage = awayTeam;
    } else {
      betMessage = "Draw";
    }
    if (position2 == 0)
      betMessage = betMessage + " O(" + overtimeMarketTrade.marketsData[0].combinedPositions[1].line / 100 + ")";
    else {
      betMessage = betMessage + " U(" + overtimeMarketTrade.marketsData[0].combinedPositions[1].line / 100 + ")";
    }
  } else if (HT_TOTAL.includes(marketId)) {

    let position1 = overtimeMarketTrade.marketsData[0].combinedPositions[0].position;
    let position2 = overtimeMarketTrade.marketsData[0].combinedPositions[1].position;
    let position3 = overtimeMarketTrade.marketsData[0].combinedPositions[2].position;

    if (position1 == 0)
      betMessage = homeTeam;
    else if (position1 == 1) {
      betMessage = awayTeam;
    } else {
      betMessage = "Draw";
    }
    if (position2 == 0)
      betMessage = betMessage + "/" + homeTeam;
    else if (position2 == 1) {
      betMessage = betMessage + "/" + awayTeam;
    } else {
      betMessage = betMessage + "/" + "Draw";
    }
    if (position3 == 0)
      betMessage = betMessage + " O(" + overtimeMarketTrade.marketsData[0].combinedPositions[2].line / 100 + ")";
    else {
      betMessage = betMessage + " U(" + overtimeMarketTrade.marketsData[0].combinedPositions[2].line / 100 + ")";
    }

  } else {

    let points = "";
    if (overtimeMarketTrade.marketsData[0].line && overtimeMarketTrade.marketsData[0].line > 0) {
      points = " @ " + overtimeMarketTrade.marketsData[0].line / 100;
    }

    if (position == 0)
      betMessage = "YES" + points;
    else {
      betMessage = "NO" + points;
    }
  }
  return {marketType, marketMessage, betMessage};
}

async function getV2ParlayMessage(overtimeMarketTrade, parlayMessage,typeMap) {
  for (const marketsData of overtimeMarketTrade.marketsData) {
    let specificGame = await axios.get('https://api.thalesmarket.io/overtime-v2/games-info/' + marketsData.gameId);
    specificGame = specificGame.data;
    let position = marketsData.position;
    let marketType = typeMap.get(marketsData.typeId).name;
    let homeTeam;
    let awayTeam;
    let marketId = typeMap.get(marketsData.typeId).id;
    if (marketsData.playerId && marketsData.playerId > 0) {
      let specificPlayer = await axios.get('https://api.thalesmarket.io/overtime-v2/players-info/' + marketsData.playerId);
      specificPlayer = specificPlayer.data.playerName;
      let betMessage = "";
      if (TOTAL.includes(marketId)) {
        if (position == 0)
          betMessage = "O(" + marketsData.line / 100 + ")";
        else {
          betMessage = "U(" + marketsData.line / 100 + ")";

        }
      } else {
        if (position == 0) {
          betMessage = "YES"
        } else
          betMessage = "NO"
      }
      parlayMessage = parlayMessage + specificPlayer + " : " + marketType + " @ " + betMessage + "\n";
    } else {
      if (specificGame.teams[0].isHome) {
        homeTeam = specificGame.teams[0].name;
        awayTeam = specificGame.teams[1].name;
      } else {
        awayTeam = specificGame.teams[0].name;
        homeTeam = specificGame.teams[1].name
      }
      let betMessage;
      if (WINNER.includes(marketId)) {
        if (position == 0)
          betMessage = homeTeam;
        else if (position == 1) {
          betMessage = awayTeam;
        } else {
          betMessage = "Draw";
        }
      }  else if(CORRECT_SCORE_LIST.includes(marketId)){
        betMessage =  CORRECT_SCORE_MAP.get(position);
      } else if(WINNING_ROUND.includes(marketId)){
        switch (position) {
          case 0:
            betMessage = "Draw";
          case 1:
            betMessage = "By decision";
          default: {
            let round = position -1;
            betMessage = "Round: "+round;
          }
        }
      }
      else if(ENDING_METHOD.includes(position)){
        switch (position) {
          case 0:
            betMessage = "Draw";
          case 1:
            betMessage = "By decision";
          case 2:
            betMessage = "KO/TKO/DQ";
          case 3:
            betMessage = "Submission";
          default: betMessage = "";
        }
      }
      else if(METHOD_OF_VICTORY.includes(marketId)){
        switch (position) {
          case 0:
            return 'Draw';
          case 1:
            return homeTeam+" (Decision)";
          case 2:
            return homeTeam+" (KO/TKO/DQ)";
          case 3:
            return homeTeam+" (Submission)";
          case 4:
            return awayTeam+" (Decision)";
          case 5:
            return awayTeam+" (KO/TKO/DQ)";
          case 6:
            return awayTeam+" (Submission)";
          default:
            return '';
        }
      } else if (HANDICAP.includes(marketId)) {
        if (isTenisV2(marketsData.sportId)) {
          if (position == 0) {
            if (marketsData.typeId == "10001") {
              betMessage = "H1(" + marketsData.line / 100 + ") games"
            } else {
              betMessage = "H1(" + marketsData.line / 100 + ") sets"
            }
          } else {
            if (marketsData.typeId == "10001") {
              betMessage = "H2(" + marketsData.line / 100 + ") games"
            } else {
              betMessage = "H2(" + marketsData.line / 100 + ") sets"
            }
          }
        } else {
          if (position == 0) {
            betMessage = "H1(" + marketsData.line / 100 + ")";
          } else {
            betMessage = "H2(" + marketsData.line / 100 + ")";
          }
        }
      } else if (TOTAL.includes(marketId)) {
        if (isTenisV2(marketsData.sportId)) {
          if (position == 0)
            if (marketsData.typeId == "10014") {
              betMessage = "O(" + marketsData.line / 100 + ") sets";
            } else {
              betMessage = "O(" + marketsData.line / 100 + ") games"
            }
          else {
            if (marketsData.typeId == "10014") {
              betMessage = "U(" + marketsData.line / 100 + ") sets";
            } else {
              betMessage = "U(" + marketsData.line / 100 + ") games";
            }
          }

        } else {
          if (position == 0)
            betMessage = "O(" + marketsData.line / 100 + ")";
          else {
            betMessage = "U(" + marketsData.line / 100 + ")";
          }
        }

      } else if (DRAW_NO_BET.includes(marketId)) {
        if (position == 0)
          betMessage = homeTeam;
        else {
          betMessage = awayTeam;
        }
      } else if (TOTAL_HOME_FIRST.includes(marketId)) {
        if (position == 0)
          betMessage = homeTeam + " O(" + marketsData.line / 100 + ")";
        else {
          betMessage = homeTeam + " U(" + marketsData.line / 100 + ")";
        }
      } else if (TOTAL_HOME_SECOND.includes(marketId)) {
        if (position == 0)
          betMessage = homeTeam + " O(" + marketsData.line / 100 + ")";
        else {
          betMessage = homeTeam + " U(" + marketsData.line / 100 + ")";
        }
      } else if (TOTAL_AWAY_SECOND.includes(marketId)) {
        if (position == 0)
          betMessage = awayTeam + " O(" + marketsData.line / 100 + ")";
        else {
          betMessage = awayTeam + " U(" + marketsData.line / 100 + ")";
        }
      } else if (TOTAL_AWAY_FIRST.includes(marketId)) {
        if (position == 0)
          betMessage = awayTeam + " O(" + marketsData.line / 100 + ")";
        else {
          betMessage = awayTeam + " U(" + marketsData.line / 100 + ")";
        }
      } else if (DOUBLE_CHANCE.includes(marketId)) {
        if (position == 0)
          betMessage = "1X";
        else if (position == 2) {
          betMessage = "X2";
        } else {
          betMessage = "1 or 2";
        }
      } else if (HALF_END.includes(marketId)) {
        let position1 = marketsData.combinedPositions[0].position;
        let position2 = marketsData.combinedPositions[1].position;

        if (position1 == 0)
          betMessage = homeTeam;
        else if (position1 == 1) {
          betMessage = awayTeam;
        } else {
          betMessage = "Draw";
        }
        if (position2 == 0)
          betMessage = betMessage + "/" + homeTeam;
        else if (position2 == 1) {
          betMessage = betMessage + "/" + awayTeam;
        } else {
          betMessage = betMessage + "/" + "Draw";
        }
      } else if (W_TOTAL.includes(marketId)) {

        let position1 = marketsData.combinedPositions[0].position;
        let position2 = marketsData.combinedPositions[1].position;

        if (position1 == 0)
          betMessage = homeTeam;
        else if (position1 == 1) {
          betMessage = awayTeam;
        } else {
          betMessage = "Draw";
        }
        if (position2 == 0)
          betMessage = betMessage + " O(" + marketsData.combinedPositions[1].line / 100 + ")";
        else {
          betMessage = betMessage + " U(" + marketsData.combinedPositions[1].line / 100 + ")";
        }
      } else if (HT_TOTAL.includes(marketId)) {

        let position1 = marketsData.combinedPositions[0].position;
        let position2 = marketsData.combinedPositions[1].position;
        let position3 = marketsData.combinedPositions[2].position;

        if (position1 == 0)
          betMessage = homeTeam;
        else if (position1 == 1) {
          betMessage = awayTeam;
        } else {
          betMessage = "Draw";
        }
        if (position2 == 0)
          betMessage = betMessage + "/" + homeTeam;
        else if (position2 == 1) {
          betMessage = betMessage + "/" + awayTeam;
        } else {
          betMessage = betMessage + "/" + "Draw";
        }
        if (position3 == 0)
          betMessage = betMessage + " O(" + marketsData.combinedPositions[2].line / 100 + ")";
        else {
          betMessage = betMessage + " U(" + marketsData.combinedPositions[2].line / 100 + ")";
        }

      } else {
        if (position == 0)
          betMessage = "YES";
        else {
          betMessage = "NO";
        }
      }

      parlayMessage = parlayMessage + homeTeam + " - " + awayTeam + " : " + marketType + " @ " + betMessage + "\n";
    }
  }
  return parlayMessage;
}

async function getOvertimeV2Trades(){

  let activeTickets = await v2Contract.methods.getActiveTickets(0,10000).call();
  let overtimeTrades;
  if (activeTickets.length>900){
    overtimeTrades = await v2TicketContract.methods.getTicketsData(activeTickets.slice(0, 900)).call();
    overtimeTrades = overtimeTrades.concat(await v2TicketContract.methods.getTicketsData(activeTickets.slice(900, 1800)).call());
  } else {
    overtimeTrades = await v2TicketContract.methods.getTicketsData(activeTickets).call();
  }let typeInfoMap = await axios.get('https://api.thalesmarket.io/overtime-v2/market-types');//api.thalesmarket.io/overtime-v2/market-types;
  let sportsInfoMap = await axios.get('https://api.thalesmarket.io/overtime-v2/sports');
  typeInfoMap = typeInfoMap.data;
  sportsInfoMap = sportsInfoMap.data;
  const sportMap = new Map(Object.entries(JSON.parse(JSON.stringify(sportsInfoMap))));
  const typeMap = new Map(Object.entries(JSON.parse(JSON.stringify(typeInfoMap))));
  var startdate = new Date();
  var durationInMinutes = 30;
  startdate.setMinutes(startdate.getMinutes() - durationInMinutes);
  let startDateUnixTime = Math.floor(startdate.getTime());
  console.log("##### length before is "+overtimeTrades.length);
  overtimeTrades = await overtimeTrades.filter(item => !writenOvertimeV2Trades.includes(item.id) && startDateUnixTime < Number(item.createdAt * 1000));
  console.log("##### length is "+overtimeTrades.length);
  let overtimeTradesUQ = await overtimeTrades.filter((value, index, self) =>
          index === self.findIndex((t) => (
              t.id == value.id || t.createdAt == value.createdAt
          ))
  )
  for (const overtimeMarketTrade of overtimeTradesUQ) {
    if (startDateUnixTime < Number(overtimeMarketTrade.createdAt * 1000) && !writenOvertimeV2Trades.includes(overtimeMarketTrade.id)) {
      try {
        let ticketOwner;
        if(overtimeMarketTrade.ticketOwner == STAKING_OP ){
          ticketOwner =  await v2StakingOP.methods.ticketToUser(overtimeMarketTrade.id).call();
        } else if (overtimeMarketTrade.ticketOwner == FREE_BET_OP) {
          ticketOwner =  await v2FreeBetgOP.methods.ticketToUser(overtimeMarketTrade.id).call();
        } else
        {
          ticketOwner = overtimeMarketTrade.ticketOwner;
        }

        let moneySymbol;
        let multiplier = 1;
        if (overtimeMarketTrade.collateral=="0x4200000000000000000000000000000000000006"){
          moneySymbol = "weth";
          multiplier = ethPrice;
        } else if (overtimeMarketTrade.collateral=="0x217D47011b23BB961eB6D93cA9945B7501a5BB11"){
          moneySymbol = "THALES";
          multiplier = thalesPrice;
        } else {
          moneySymbol = "USDC"
        }
        let buyIn = roundTo2Decimals(formatV2Amount(overtimeMarketTrade.buyInAmount , overtimeMarketTrade.collateral));
        let odds = oddslib.from('impliedProbability', overtimeMarketTrade.totalQuote / 1e18).decimalValue.toFixed(3)
        if (overtimeMarketTrade.marketsData.length==1) {
          let {marketType, marketMessage, betMessage} = await getV2MessageContent(overtimeMarketTrade,typeMap);

          let linkTransaction;
          linkTransaction = "https://www.overtimemarkets.xyz/tickets/";


          const embed = {
            fields: [
              {
                name: "Overtime V2 Market Trade",
                value: "\u200b",
              },
              {
                name: ":classical_building: Overtime market:",
                value:
                    "[" +
                    marketMessage +
                    "](https://v2.overtimemarkets.xyz/markets/" +
                    overtimeMarketTrade.marketsData[0].gameId +
                    ")",
              },
              {
                name: ":coin: Bet type:",
                value: marketType,
              },
              {
                name: ":coin: Position:",
                value: betMessage,
              },
              {
                name: ":link: Ticket address:",
                value:
                    "[" +
                    overtimeMarketTrade.id +
                    "](" + linkTransaction +
                    overtimeMarketTrade.id +
                    ")",
              },
              {
                name: ":link: Ticket owner:",
                value:
                    "[" +
                    ticketOwner +
                    "](" + linkTransaction +
                    ticketOwner +
                    ")",
              },
              {
                name: ":coin: Buy in Amount:",
                value: buyIn + " " + moneySymbol
              },
              {
                name: ":coin: Payout:",
                value: roundTo2Decimals(buyIn * odds) + " "+ moneySymbol ,
              },
              {
                name: ":coin: Fees:",
                value: formatV2Amount(overtimeMarketTrade.fees , overtimeMarketTrade.collateral).toFixed(2) + " " + moneySymbol ,
              },
              {
                name: ":coin: Odds:",
                value: odds,
              },
              {
                name: ":alarm_clock: Game time:",
                value: new Date(overtimeMarketTrade.marketsData[0].maturity * 1000).toUTCString(),
              },
              {
                name: ":alarm_clock: Timestamp:",
                value: new Date(overtimeMarketTrade.createdAt * 1000).toUTCString(),
              },
            ],
          };

          let amountInCurrency = formatV2Amount(overtimeMarketTrade.buyInAmount , overtimeMarketTrade.collateral);
          let payoutInCurrency =  multiplier * roundTo2Decimals(buyIn * odds);
          let overtimeTradesChannel;
          if(Number(multiplier*amountInCurrency)<500){
            if(payoutInCurrency>=1000){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch("1272945193016098849");
            } else if(overtimeMarketTrade.isLive){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch("1250500936120406177");
            } else {
             overtimeTradesChannel = await clientNewListings.channels
                .fetch("1249389171311644816");
            }
          } else{
            if(overtimeMarketTrade.isLive){
             overtimeTradesChannel = await clientNewListings.channels
                .fetch("1250500964608245853");
          } else {
             overtimeTradesChannel = await clientNewListings.channels
                .fetch("1249389262089228309");
            }
          }
          await overtimeTradesChannel.send({embeds: [embed]});
          writenOvertimeV2Trades.push(overtimeMarketTrade.id);
          redisClient.lpush(overtimeV2TradesKey, overtimeMarketTrade.id);

        } else {
          let parlayMessage = "";
          parlayMessage = await getV2ParlayMessage(overtimeMarketTrade, parlayMessage,typeMap);
          let linkTransaction;
          linkTransaction = "https://www.overtimemarkets.xyz/tickets/"

          const embed = {
            fields: [
              {
                name: "Overtime V2 Market Trade",
                value: "\u200b",
              },
              {
                name: ":classical_building: Overtime market:",
                value:
                    "[" +
                    parlayMessage +
                    "](https://v2.overtimemarkets.xyz/markets/" +
                    overtimeMarketTrade.marketsData[0].gameId +
                    ")",
              },
              {
                name: ":link: Ticket address:",
                value:
                    "[" +
                    overtimeMarketTrade.id +
                    "](" + linkTransaction +
                    overtimeMarketTrade.id +
                    ")",
              },{
                name: ":link: Ticket owner:",
                value:
                    "[" +
                    ticketOwner +
                    "](" + linkTransaction +
                    ticketOwner +
                    ")",
              },
              {
                name: ":coin: Buy in Amount:",
                value: buyIn + " " + moneySymbol
              },
              {
                name: ":coin: Payout:",
                value: roundTo2Decimals(buyIn * odds) + " "+ moneySymbol ,
              },
              {
                name: ":coin: Fees:",
                value: formatV2Amount(overtimeMarketTrade.fees , overtimeMarketTrade.collateral).toFixed(2) + " " + moneySymbol,
              },
              {
                name: ":coin: Total Quote:",
                value: odds,
              },
              {
                name: ":alarm_clock: End time:",
                value: new Date(overtimeMarketTrade.expiry * 1000).toUTCString(),
              },
              {
                name: ":alarm_clock: Timestamp:",
                value: new Date(overtimeMarketTrade.createdAt * 1000).toUTCString(),
              }
            ],
          };

          let amountInCurrency = formatV2Amount(overtimeMarketTrade.buyInAmount , overtimeMarketTrade.collateral)
          let payoutInCurrency = multiplier * roundTo2Decimals(buyIn * odds);
          let overtimeTradesChannel;

          if(Number(multiplier*amountInCurrency)<500){
            if(payoutInCurrency>=1000){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch("1272945193016098849");
            } else if(overtimeMarketTrade.isLive){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch("1250500936120406177");
            } else {
             overtimeTradesChannel = await clientNewListings.channels
                .fetch("1249389171311644816");
            }} else{
            if(overtimeMarketTrade.isLive){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch("1250500964608245853");
            } else {
             overtimeTradesChannel = await clientNewListings.channels
                .fetch("1249389262089228309");
          }}

          await overtimeTradesChannel.send({embeds: [embed]});

          writenOvertimeV2Trades.push(overtimeMarketTrade.id);
          redisClient.lpush(overtimeV2TradesKey, overtimeMarketTrade.id);
        }


      } catch (e) {
        console.log("There was a problem while getting overtime V2 trades",e);
      }
    }
  }
}






let thalesPrice = 0.22;
let ethPrice = 3000;

async function updateTokenPrice() {

  let dataThales = await CoinGeckoClient.coins.fetch("thales");
  if(dataThales.data && dataThales.data.market_data) {
    thalesPrice =   dataThales.data.market_data.current_price.usd;
  }

  let dataETH = await CoinGeckoClient.coins.fetch("ethereum");
  if(dataETH.data && dataETH.data.market_data) {
    ethPrice =   dataETH.data.market_data.current_price.usd;
  }
}


async function getOvertimeV2ARBTrades(){

  let activeTickets = await v2ARBContract.methods.getActiveTickets(0,10000).call();
  let overtimeTrades = await v2ARBTicketContract.methods.getTicketsData(activeTickets).call();
  let typeInfoMap = await axios.get('https://api.thalesmarket.io/overtime-v2/market-types');//api.thalesmarket.io/overtime-v2/market-types;
  let sportsInfoMap = await axios.get('https://api.thalesmarket.io/overtime-v2/sports');
  typeInfoMap = typeInfoMap.data;
  sportsInfoMap = sportsInfoMap.data;
  const sportMap = new Map(Object.entries(JSON.parse(JSON.stringify(sportsInfoMap))));
  const typeMap = new Map(Object.entries(JSON.parse(JSON.stringify(typeInfoMap))));
  var startdate = new Date();
  var durationInMinutes = 30;
  startdate.setMinutes(startdate.getMinutes() - durationInMinutes);
  let startDateUnixTime = Math.floor(startdate.getTime());
  console.log("##### length before is "+overtimeTrades.length);
  overtimeTrades = await overtimeTrades.filter(item => !writenOvertimeV2ARBTrades.includes(item.id) && startDateUnixTime < Number(item.createdAt * 1000));
  console.log("##### length is "+overtimeTrades.length);
  let overtimeTradesUQ = await overtimeTrades.filter((value, index, self) =>
          index === self.findIndex((t) => (
              t.id == value.id || t.createdAt == value.createdAt
          ))
  )
  console.log("##### length is after dupl "+overtimeTradesUQ.length);
  console.log("##### trades are  "+ JSON.stringify(overtimeTradesUQ));
  for (const overtimeMarketTrade of overtimeTradesUQ) {
    if (startDateUnixTime < Number(overtimeMarketTrade.createdAt * 1000) && !writenOvertimeV2ARBTrades.includes(overtimeMarketTrade.id)) {
      try {
        let moneySymbol;
        let multiplier = 1;
        if (overtimeMarketTrade.collateral=="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"){
          moneySymbol = "weth";
          multiplier = ethPrice;
        } else if (overtimeMarketTrade.collateral=="0xE85B662Fe97e8562f4099d8A1d5A92D4B453bF30"){
          moneySymbol = "THALES";
          multiplier = thalesPrice;
        } else {
          moneySymbol = "USDC";
        }

        let ticketOwner;
        if(overtimeMarketTrade.ticketOwner == STAKING_ARB ){
          ticketOwner =  await v2StakingARB.methods.ticketToUser(overtimeMarketTrade.id).call();
        } else if (overtimeMarketTrade.ticketOwner == FREE_BET_ARB) {
          ticketOwner =  await v2FreeBetARB.methods.ticketToUser(overtimeMarketTrade.id).call();
        } else
        {
          ticketOwner = overtimeMarketTrade.ticketOwner;
        }

        let buyIn = roundTo2Decimals(formatV2ARBAmount(overtimeMarketTrade.buyInAmount , overtimeMarketTrade.collateral));
        let odds = oddslib.from('impliedProbability', overtimeMarketTrade.totalQuote / 1e18).decimalValue.toFixed(3)
        if (overtimeMarketTrade.marketsData.length==1) {
          let {marketType, marketMessage, betMessage} = await getV2MessageContent(overtimeMarketTrade,typeMap);

          let linkTransaction;
          linkTransaction = "https://www.overtimemarkets.xyz/tickets/";

          const embed = {
            fields: [
              {
                name: "Overtime V2 Market Trade",
                value: "\u200b",
              },
              {
                name: ":classical_building: Overtime market:",
                value:
                    "[" +
                    marketMessage +
                    "](https://v2.overtimemarkets.xyz/markets/" +
                    overtimeMarketTrade.marketsData[0].gameId +
                    ")",
              },
              {
                name: ":coin: Bet type:",
                value: marketType,
              },
              {
                name: ":coin: Position:",
                value: betMessage,
              },
              {
                name: ":link: Ticket  address:",
                value:
                    "[" +
                    overtimeMarketTrade.id +
                    "](" + linkTransaction +
                    overtimeMarketTrade.id +
                    ")",
              },{
                name: ":link: Ticket owner:",
                value:
                    "[" +
                    ticketOwner +
                    "](" + linkTransaction +
                    ticketOwner +
                    ")",
              },
              {
                name: ":coin: Buy in Amount:",
                value: buyIn + " " + moneySymbol
              },
              {
                name: ":coin: Payout:",
                value: roundTo2Decimals(buyIn * odds) + " "+ moneySymbol ,
              },
              {
                name: ":coin: Fees:",
                value: formatV2ARBAmount(overtimeMarketTrade.fees , overtimeMarketTrade.collateral).toFixed(2) + " " + moneySymbol ,
              },
              {
                name: ":coin: Odds:",
                value: odds,
              },
              {
                name: ":alarm_clock: Game time:",
                value: new Date(overtimeMarketTrade.marketsData[0].maturity * 1000).toUTCString(),
              },
              {
                name: ":alarm_clock: Timestamp:",
                value: new Date(overtimeMarketTrade.createdAt * 1000).toUTCString(),
              }
            ],
          };

          let amountInCurrency = formatV2ARBAmount(overtimeMarketTrade.buyInAmount , overtimeMarketTrade.collateral);
          let payoutInCurrency = multiplier * roundTo2Decimals(buyIn * odds);
          let overtimeTradesChannel;
          if(Number(multiplier*amountInCurrency)<500){
            if(payoutInCurrency>=1000){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch("1272950140637806705");
            } else if(overtimeMarketTrade.isLive){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch("1272539526274744390");

            } else {
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch("1272539024464281622");
             }
          } else{
            if(overtimeMarketTrade.isLive){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch("1272539696672804907");

            } else {
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch("1272539294258561045");
               }
          }
          if(!writenOvertimeV2ARBTrades.includes(overtimeMarketTrade.id)){
          writenOvertimeV2ARBTrades.push(overtimeMarketTrade.id);
          let newVar1 = await redisClient.lpush(overtimeV2ARBTradesKey, overtimeMarketTrade.id);
          let newVar = await overtimeTradesChannel.send({ embeds: [embed] });
          console.log("#@#@#@Sending arb message: "+JSON.stringify(embed));
          }
        } else {
          let parlayMessage = "";
          parlayMessage = await getV2ParlayMessage(overtimeMarketTrade, parlayMessage,typeMap);
          let linkTransaction;
          linkTransaction = "https://www.overtimemarkets.xyz/tickets/"

          const embed = {
            fields: [
              {
                name: "Overtime V2 Market Trade",
                value: "\u200b",
              },
              {
                name: ":classical_building: Overtime market:",
                value:
                    "[" +
                    parlayMessage +
                    "](https://v2.overtimemarkets.xyz/markets/" +
                    overtimeMarketTrade.marketsData[0].gameId +
                    ")",
              },
              {
                name: ":link: Ticket address:",
                value:
                    "[" +
                    overtimeMarketTrade.id +
                    "](" + linkTransaction +
                    overtimeMarketTrade.id +
                    ")",
              },{
                name: ":link: Ticket owner:",
                value:
                    "[" +
                    ticketOwner +
                    "](" + linkTransaction +
                    ticketOwner +
                    ")",
              },
              {
                name: ":coin: Buy in Amount:",
                value: buyIn + " " + moneySymbol
              },
              {
                name: ":coin: Payout:",
                value: roundTo2Decimals(buyIn * odds) + " "+ moneySymbol ,
              },
              {
                name: ":coin: Fees:",
                value: formatV2ARBAmount(overtimeMarketTrade.fees , overtimeMarketTrade.collateral).toFixed(2) + " " + moneySymbol,
              },
              {
                name: ":coin: Total Quote:",
                value: odds,
              },
              {
                name: ":alarm_clock: End time:",
                value: new Date(overtimeMarketTrade.expiry * 1000).toUTCString(),
              },
              {
                name: ":alarm_clock: Timestamp:",
                value: new Date(overtimeMarketTrade.createdAt * 1000).toUTCString(),
              }
            ],
          };

          let amountInCurrency = formatV2ARBAmount(overtimeMarketTrade.buyInAmount , overtimeMarketTrade.collateral)
          let payoutInCurrency = multiplier * roundTo2Decimals(buyIn * odds);
          let overtimeTradesChannel;
          if(Number(multiplier*amountInCurrency)<500){
            if(payoutInCurrency>=1000){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch("1272950140637806705");
            } else if(overtimeMarketTrade.isLive){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch("1272539526274744390");
            } else {
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch("1272539024464281622");
            }
          } else{
            if(overtimeMarketTrade.isLive){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch("1272539696672804907");
            } else {
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch("1272539294258561045");
               }}


          if(!writenOvertimeV2ARBTrades.includes(overtimeMarketTrade.id)) {
            writenOvertimeV2ARBTrades.push(overtimeMarketTrade.id);
            let lpush = await redisClient.lpush(overtimeV2ARBTradesKey, overtimeMarketTrade.id);
           let newVar = await overtimeTradesChannel.send({ embeds: [embed] });
            console.log("#@#@#@Sending arb message: "+JSON.stringify(embed));
          }
        }


      } catch (e) {
        console.log("There was a problem while getting overtime V2 trades arb #@#@#@@#@",e);
      }
    }
  }
}
