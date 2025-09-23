require("dotenv").config();
const {
  Client,
  Intents,
  GatewayIntentBits,
  EmbedBuilder,
  ActivityType,
  ChannelType
} = require("discord.js");
const clientNewListings = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.DirectMessages
  ],
});
clientNewListings.login(process.env.BOT_TOKEN_LISTINGS);
const clientOVERCirculating = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildMessages
  ],
})
clientOVERCirculating.login(process.env.BOT_TOKEN_CIRCULATING_OVER);
const redis = require("redis");
var fs = require("fs");
const axios = require("axios");
const { v4: uuidv4 } = require('uuid');
const SYNTH_USD_MAINNET = "0x57ab1ec28d129707052df4df418d58a2d46d5f51";
const Web3 = require("web3");


const web3OP   = new Web3(new Web3.providers.HttpProvider(process.env.DRPC_OP_URL,   { timeout: 30000 }));
const web3ARB  = new Web3(new Web3.providers.HttpProvider(process.env.DRPC_ARB_URL,  { timeout: 30000 }));
const web3BASE = new Web3(new Web3.providers.HttpProvider(process.env.DRPC_BASE_URL, { timeout: 30000 }));

const path = require('path');
const puppeteer = require('puppeteer');
const cron = require('node-cron');

const multipliersUrl = 'https://overdrop.overtime.io/game-multipliers';
const networkId = 10; // Optimism
const adminApiKey = process.env.OVER_API_KEY;
const tz = 'Etc/GMT-1';
const DISCORD_ANNOUNCEMENTS = '906495542744469544';
const DISCORD_MONEY_TIPS     = '1407664919033413705';



let arbitrumRaw = fs.readFileSync('contracts/arbitrum.json');
let arbitrumContract = JSON.parse(arbitrumRaw);
const web3 = new Web3(new Web3.providers.HttpProvider(process.env.INFURA_URL));
const CoinGecko = require('coingecko-api');
const CoinGeckoClient = new CoinGecko();
const oddslib = require('oddslib');


let contractV2OTRaw = fs.readFileSync('contracts/v2overtime.json');
let v2ContractRaw = JSON.parse(contractV2OTRaw);
let v2Contract = new web3OP.eth.Contract(v2ContractRaw,"0x2367FB44C4C2c4E5aAC62d78A55876E01F251605");
let FREE_BET_ARB = "0xd1F2b87a9521315337855A132e5721cfe272BBd9";
let STAKING_ARB = "0x109e966A4d856B82f158BF528395de6fF36214A8";
let overtimeV2TradesKey = "overtimeV2TradesKey";
let overtimeV2BigWinsTradesKey = "overtimeV2BigWinsTradesKey";
let overtimeV2ARBTradesKey = "overtimeV2ARBTradesKey";
let overtimeV2BASETradesKey = "overtimeV2BASETradesKey";
let FREE_BET_OP = "0x8D18e68563d53be97c2ED791CA4354911F16A54B";
let STAKING_OP = "0x5e6D44B17bc989652920197790eF626b8a84e219";
let MOLLY_BETS ="0x1cA826587Ee86E44F2D8517E0F1A376F6dABC9c0";

let FREE_BET_BASE = "0x2929Cf1edAc2DB91F68e2822CEc25736cAe029bf";
let STAKING_BASE = "0x84aB38e42D8Da33b480762cCa543eEcA6135E040";

let contractStakingTRaw = fs.readFileSync('contracts/staking.json');
let v2StakingRaw = JSON.parse(contractStakingTRaw);

let v2StakingARB = new web3ARB.eth.Contract(v2StakingRaw,STAKING_ARB);
let v2StakingOP = new web3OP.eth.Contract(v2StakingRaw,STAKING_OP);
let v2StakingBASE = new web3BASE.eth.Contract(v2StakingRaw,STAKING_BASE);

let v2FreeBetARB = new web3ARB.eth.Contract(v2StakingRaw,FREE_BET_ARB);
let v2FreeBetgOP = new web3OP.eth.Contract(v2StakingRaw,FREE_BET_OP);
let v2FreeBetgBASE = new web3BASE.eth.Contract(v2StakingRaw,FREE_BET_BASE);


let contractV2TicketRaw = fs.readFileSync('contracts/v2overtimeTicket.json');
let v2ContractTicketRaw = JSON.parse(contractV2TicketRaw);
let v2TicketContract = new web3OP.eth.Contract(v2ContractTicketRaw,"0x71CE219942FFD9C1d8B67d6C35C39Ae04C4F647B");

let v2ARBContract = new web3ARB.eth.Contract(v2ContractRaw,"0xB155685132eEd3cD848d220e25a9607DD8871D38");

let v2ARBTicketContract = new web3ARB.eth.Contract(v2ContractTicketRaw,"0x04386f9b2b4f713984Fe0425E46a376201641649");

let v2BASEContract = new web3BASE.eth.Contract(v2ContractRaw,"0xA2dCFEe657Bc0a71AC31d146366246202eae18a4");


let contractESORaw = fs.readFileSync('contracts/eso.json');
let esoContrat = JSON.parse(contractESORaw);
let esoOPContract = new web3OP.eth.Contract(esoContrat,"0x0000001c5b32f37f5bea87bdd5374eb2ac54ea8e");
let esoARBContract = new web3ARB.eth.Contract(esoContrat,"0x0000001c5b32f37f5bea87bdd5374eb2ac54ea8e");
let esoBASEContract = new web3BASE.eth.Contract(esoContrat,"0x0000001c5b32f37f5bea87bdd5374eb2ac54ea8e");

let v2BASETicketContract = new web3BASE.eth.Contract(v2ContractTicketRaw,"0x99D318b6402cE95B6B46F40f752eA96430A2Ead0");

// Global Channel IDs
const CHANNEL_OPT_SMALL = "1249389171311644816";
const CHANNEL_OPT_LIVE_SMALL = "1250500936120406177";
const CHANNEL_OPT_LARGE = "1249389262089228309";
const CHANNEL_OPT_LIVE_LARGE = "1250500964608245853";
const CHANNEL_OPT_BIG_PAYOUT = "1272945193016098849";

const CHANNEL_ARB_SMALL = "1272539024464281622";
const CHANNEL_ARB_LIVE_SMALL = "1272539526274744390";
const CHANNEL_ARB_LARGE = "1272539294258561045";
const CHANNEL_ARB_LIVE_LARGE = "1272539696672804907";
const CHANNEL_ARB_BIG_PAYOUT = "1272950140637806705";

const CHANNEL_BASE_SMALL = "1334190781115928677";
const CHANNEL_BASE_LIVE_SMALL = "1334191049454780478";
const CHANNEL_BASE_LARGE = "1334190954969825300";
const CHANNEL_BASE_LIVE_LARGE = "1334191112310489210";
const CHANNEL_BASE_BIG_PAYOUT = "1334191185694294076";

const MOLLY_CHANNEL_ID_ARB = "1414626884934828102";
const MOLLY_CHANNEL_ID_OP = "1414626846246568007";
const MOLLY_CHANNEL_ID_BASE = "1414627646003740775";

const userDukaIdForBigTrades = '340872297630138370';
const userLukaIdForBigTrades = '696035982394523799';
const userPedjaIdForBigTrades = '1419388259615768648';

const ALL_CHANNEL_IDS = [
  CHANNEL_OPT_SMALL,
  CHANNEL_OPT_LIVE_SMALL,
  CHANNEL_OPT_LARGE,
  CHANNEL_OPT_LIVE_LARGE,
  CHANNEL_OPT_BIG_PAYOUT,
  CHANNEL_ARB_SMALL,
  CHANNEL_ARB_LIVE_SMALL,
  CHANNEL_ARB_LARGE,
  CHANNEL_ARB_LIVE_LARGE,
  CHANNEL_ARB_BIG_PAYOUT,
  CHANNEL_BASE_SMALL,
  CHANNEL_BASE_LIVE_SMALL,
  CHANNEL_BASE_LARGE,
  CHANNEL_BASE_LIVE_LARGE,
  CHANNEL_BASE_BIG_PAYOUT
];

let writenOvertimeV2Trades = [];
let writenBigWinTicketsOvertimeV2Trades = [];
let writenOvertimeV2ARBTrades = [];

let writenOvertimeV2BASETrades = [];
let redisClient;
if (process.env.REDIS_URL) {
  redisClient = redis.createClient(process.env.REDIS_URL);
  redisClient.on("error", function (error) {
    console.error(error);
  });

  redisClient.lrange(overtimeV2TradesKey, 0, -1, function (err, polygonTrades) {
    writenOvertimeV2Trades = polygonTrades;
  });

  redisClient.lrange(overtimeV2BigWinsTradesKey, 0, -1, function (err, polygonTrades) {
    writenBigWinTicketsOvertimeV2Trades = polygonTrades;
  });


  redisClient.lrange(overtimeV2ARBTradesKey, 0, -1, function (err, polygonTrades) {
    writenOvertimeV2ARBTrades = polygonTrades;
  });

  redisClient.lrange(overtimeV2BASETradesKey, 0, -1, function (err, polygonTrades) {
    writenOvertimeV2BASETrades = polygonTrades;
  });



}

const WETH_ADDRESS_BASE = "0x4200000000000000000000000000000000000006";

const THALES_ADDRESS_OP = "0x217D47011b23BB961eB6D93cA9945B7501a5BB11";

let typeInfoMap;

async function fetchTypeInfoMap() {
  try {
    const response = await axios.get('https://api.overtime.io/overtime-v2/market-types');
    typeInfoMap = response.data;

  } catch (error) {
    console.error('Error fetching market types:', error.message);
  }
}

// Call it once immediately on startup
fetchTypeInfoMap();

// Then call it every 5 minutes
setInterval(fetchTypeInfoMap, 5 * 60 * 1000);



function  formatV2Amount(numberForFormating, collateralAddress) {

  if(collateralAddress == WETH_ADDRESS_BASE || collateralAddress == THALES_ADDRESS_OP || collateralAddress.toLowerCase() == OVER_ADDRESS_OP.toLowerCase()) {
    return numberForFormating / 1e18;
  } else {
    return numberForFormating / 1e6
  }
}

const THALES_ADDRESS_BASE = "0x1527d463cC46686f815551314BD0E5Af253d58C0";

const BITCOIN_ADDRESS_BASE = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf";

function  formatV2BASEAmount(numberForFormating, collateralAddress) {

  if(collateralAddress == WETH_ADDRESS_BASE || collateralAddress == THALES_ADDRESS_BASE || collateralAddress.toLowerCase() == OVER_ADDRESS_BASE.toLowerCase()) {
    return numberForFormating / 1e18;
  } else if(collateralAddress == BITCOIN_ADDRESS_BASE){
    return numberForFormating / 1e8;
  }else {
    return numberForFormating / 1e6
  }
}


const BITCOIN_ADDRESS_ARB = "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f";

function  formatV2ARBAmount(numberForFormating, collateralAddress) {

  if(collateralAddress == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831") {
    return numberForFormating / 1e6;
  } else if(collateralAddress == BITCOIN_ADDRESS_ARB){
    return numberForFormating / 1e8;
  }else {
    return numberForFormating / 1e18
  }
}




async function sendMessageIfNotDuplicate(channel, embed, uniqueValue, additionalText,mollyChannel) {
  const messages = await channel.messages.fetch({ limit: 100 });

  const duplicate = messages.find(msg => {
    return msg.embeds.some(embed => {
      if (!embed.fields || embed.fields.length === 0) return false;

      return embed.fields.some(field =>
          field.value.includes(uniqueValue)
      );
    });
  });

  if (duplicate) {
    console.log(`Embed with value "${uniqueValue}" already exists.`);
    return;
  }
  if(mollyChannel){
  mollyChannel.send({ embeds: [embed] }).catch(console.error);
  }

  channel.send({ embeds: [embed] }).catch(console.error);
  if(additionalText){
    embed.fields.push(
        {
          name: ":coin: TRADE TYPE (DM):",
          value: additionalText,
        }
    );
    await sendDirectMessageToUser(embed, additionalText);
  }
}

async function sendDirectMessageToUser(embed, additionalText) {
  try {
    const userDuka = await clientNewListings.users.fetch(userDukaIdForBigTrades);
    const userLuka = await clientNewListings.users.fetch(userLukaIdForBigTrades);
    const userPedja = await clientNewListings.users.fetch(userPedjaIdForBigTrades);

    const dmEmbed = { ...embed, fields: [...embed.fields] };

    if (dmEmbed.fields && dmEmbed.fields.length > 0) {
      dmEmbed.fields[0] = {
        ...dmEmbed.fields[0],
        value: dmEmbed.fields[0].value + (" "+additionalText)
      };
    }

    await userLuka.send({ embeds: [dmEmbed] });
    await userDuka.send({ embeds: [dmEmbed] });
    await userPedja.send({ embeds: [dmEmbed] });
    console.log(`✅ Embed DM sent to ${userDuka.tag}`);
  } catch (error) {
    console.error(`❌ Could not send DM to user ID:`, error);
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
TOTAL_HOME_FIRST.push(10111,10211,10311,10411,10511,10611,10711,10811)

    let TOTAL_HOME_SECOND = new Array;
TOTAL_HOME_SECOND.push(10211, 10017)

    let TOTAL_AWAY_SECOND = new Array;
TOTAL_AWAY_SECOND.push(10212,10018,10112,10312,10412,10512,10612,10712,10812)

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
  updateCirculatingAndMarketCap();
});

setInterval(function () {
  console.log("get L2 trades");
  getOvertimeV2Trades();
  getOvertimeV2BASETrades();
  getOvertimeV2ARBTrades();
}, 2 * 60 * 1000);

setInterval(function () {
  console.log("cleaning duplicates");
  cleanUpDuplicateMessages();
}, 1 * 60 * 1000);

setInterval(function () {
  console.log("update prices");
  updateTokenPrice();
  updateCirculatingAndMarketCap();
}, 10 * 60 * 1000);


/*setInterval(function () {
  console.log("get L2 trades");

}, 6 * 60 * 1000);

setInterval(function () {
  console.log("get L2 trades");

}, 7 * 60 * 1000);*/

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
  let specificGame = await axios.get('https://api.overtime.io/overtime-v2/games-info/' + overtimeMarketTrade.marketsData[0].gameId);
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
    let specificPlayer = await axios.get('https://api.overtime.io/overtime-v2/players-info/' + overtimeMarketTrade.marketsData[0].playerId);
    specificPlayer = specificPlayer.data.playerName;
    marketMessage = specificPlayer;
  } else {
    marketMessage = homeTeam + " - " + awayTeam;
  }

  if(!marketMessage){
    console.log("### Market message is empty for id "+overtimeMarketTrade.id);
    marketMessage = "overtime"
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
  else if(ENDING_METHOD.includes(marketId)){
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
          betMessage = "H2(" + (-overtimeMarketTrade.marketsData[0].line / 100) + ") games"
        } else {
          betMessage = "H2(" + (-overtimeMarketTrade.marketsData[0].line / 100) + ") sets"
        }
      }
    } else {
      if (position == 0) {
        betMessage = "H1(" + overtimeMarketTrade.marketsData[0].line / 100 + ")";
      } else {
        betMessage = "H2(" + (-overtimeMarketTrade.marketsData[0].line / 100) + ")";
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
    let specificGame = await axios.get('https://api.overtime.io/overtime-v2/games-info/' + marketsData.gameId);
    specificGame = specificGame.data;
    let position = marketsData.position;
    let marketType = typeMap.get(marketsData.typeId).name;
    let homeTeam;
    let awayTeam;
    let marketId = typeMap.get(marketsData.typeId).id;
    if (marketsData.playerId && marketsData.playerId > 0) {
      let specificPlayer = await axios.get('https://api.overtime.io/overtime-v2/players-info/' + marketsData.playerId);
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
              betMessage = "H2(" + (-marketsData.line / 100) + ") games"
            } else {
              betMessage = "H2(" + (-marketsData.line / 100) + ") sets"
            }
          }
        } else {
          if (position == 0) {
            betMessage = "H1(" + marketsData.line / 100 + ")";
          } else {
            betMessage = "H2(" + (-marketsData.line / 100) + ")";
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
  if(!parlayMessage){
    console.log("### Parlay message is empty for id "+overtimeMarketTrade.id);
    parlayMessage = "overtime parlay";
  }
  return parlayMessage;
}

const OVER_ADDRESS_OP = "0xedf38688b27036816a50185caa430d5479e1c63e";

const CHANNEL_OP_BIG_WIN = "1415088775528190032";
const CHANNEL_BASE_BIG_WIN = "1415089227904848013";
const CHANNEL_ARB_BIG_WIN = "1415088972673056799";
function tradeTimeMs(t) {
  // Prefer ticket expiry when present (parlays/system tickets),
  // otherwise use the max maturity across legs.
  if (t && t.expiry != null) return Number(t.expiry) * 1000;

  if (Array.isArray(t?.marketsData) && t.marketsData.length) {
    let maxMat = 0;
    for (const m of t.marketsData) {
      const matMs = Number(m?.maturity) * 1000;
      if (!Number.isNaN(matMs)) maxMat = Math.max(maxMat, matMs);
    }
    return maxMat;
  }
  return 0; // unknown -> treat as old
}


async function getOvertimeV2Trades(){

  if (!typeInfoMap) return;
  const activeTickets = await getAllActiveTicketsPaged(v2Contract);
  let overtimeTrades = await robustGetTicketsData(v2TicketContract, activeTickets,'OP:getTicketsData');

  const typeMap = new Map(Object.entries(JSON.parse(JSON.stringify(typeInfoMap))));
  var startdate = new Date();
  var durationInMinutes = 30;
  startdate.setMinutes(startdate.getMinutes() - durationInMinutes);
  let startDateUnixTime = Math.floor(startdate.getTime());
  console.log("##### length before is "+overtimeTrades.length);
  overtimeTrades = overtimeTrades.filter(item => {
    const isNewNormal =
        startDateUnixTime < Number(item.createdAt * 1000) &&
        !writenOvertimeV2Trades.includes(item.id);

    return isNewNormal;
  });console.log("##### length is "+overtimeTrades.length);
  let overtimeTradesUQ = await overtimeTrades.filter((value, index, self) =>
          index === self.findIndex((t) => (
              t.id == value.id || t.createdAt == value.createdAt
          ))
  )
  for (const overtimeMarketTrade of overtimeTradesUQ) {
    if ((startDateUnixTime < Number(overtimeMarketTrade.createdAt * 1000) && !writenOvertimeV2Trades.includes(overtimeMarketTrade.id))) {
      try {

        if (
            !overtimeMarketTrade ||
            !Array.isArray(overtimeMarketTrade.marketsData) ||
            overtimeMarketTrade.marketsData.length === 0
        ) {
          console.warn(`⚠️ Skipping trade ${overtimeMarketTrade?.id} — no marketsData`);
          continue; // skip to the next trade
        }const address = Web3.utils.toChecksumAddress(overtimeMarketTrade.ticketOwner);
        let isSmart =  await isSmartContract(web3OP, address);
        let smartTicketOwner;
        if (isSmart){
          smartTicketOwner = await getOwnerOfSmartAccount(esoOPContract,overtimeMarketTrade.ticketOwner);
        }

        let ticketOwner;
        if(overtimeMarketTrade.ticketOwner == STAKING_OP ){
          ticketOwner =  await v2StakingOP.methods.ticketToUser(overtimeMarketTrade.id).call();
        } else if (overtimeMarketTrade.ticketOwner == FREE_BET_OP) {
          ticketOwner =  await v2FreeBetgOP.methods.ticketToUser(overtimeMarketTrade.id).call();
        } else
        {
          ticketOwner = overtimeMarketTrade.ticketOwner;
        }

        let mollyChannel;
        if(MOLLY_BETS.toLowerCase() == overtimeMarketTrade.ticketOwner.toLowerCase()){
          mollyChannel = await clientNewListings.channels
              .fetch(MOLLY_CHANNEL_ID_OP);
          console.log("##### Molly bet detected for ticket "+overtimeMarketTrade.id);
        }

        let isSystem = overtimeMarketTrade.isSystem;
        let mustWins;
        if(isSystem){
          mustWins = Number(overtimeMarketTrade.systemBetDenominator)+"/"+overtimeMarketTrade.marketsData.length;
        }else {
          mustWins = overtimeMarketTrade.marketsData.length+"/"+overtimeMarketTrade.marketsData.length;
        }
        let moneySymbol;
        let multiplier = 1;
        if (overtimeMarketTrade.collateral==WETH_ADDRESS_BASE){
          moneySymbol = "weth";
          multiplier = ethPrice;
        } else if (overtimeMarketTrade.collateral== THALES_ADDRESS_OP){
          moneySymbol = "OVER";
          multiplier = thalesPrice;
        } else if (overtimeMarketTrade.collateral.toLowerCase() == OVER_ADDRESS_OP.toLowerCase()){
          moneySymbol = "OVER";
          multiplier = thalesPrice;
        } else {
          moneySymbol = "USDC"
        }
        let buyIn = roundTo2Decimals(formatV2Amount(overtimeMarketTrade.buyInAmount , overtimeMarketTrade.collateral));
        let odds = oddslib.from('impliedProbability', overtimeMarketTrade.totalQuote / 1e18).decimalValue.toFixed(3);
        let amountInCurrency = formatV2Amount(overtimeMarketTrade.buyInAmount , overtimeMarketTrade.collateral)
        let payoutInCurrency = multiplier * roundTo2Decimals(buyIn * odds);
        let buyInAmountUSD = "";
        let payoutInUSD = "";
        if (multiplier !== 1) {
          buyInAmountUSD = " ("+roundTo2Decimals(amountInCurrency * multiplier) + " $)";
          payoutInUSD = " ("+roundTo2Decimals(payoutInCurrency) + " $)";
        }


        let isSGP = await v2Contract.methods.isSGPTicket(overtimeMarketTrade.id).call();
        if (overtimeMarketTrade.marketsData.length==1) {
          let content;
          try {
            content = await getV2MessageContent(overtimeMarketTrade, typeMap);
          } catch (err) {
            console.warn(`⚠️ OP Skipping trade ${overtimeMarketTrade?.id} in getV2MessageContent:`, err?.message || err);
            continue; // skip this trade
          }
          if (!content) {
            console.warn(`⚠️ OP Skipping trade ${overtimeMarketTrade?.id} — empty content from getV2MessageContent`);
            continue;
          }
          const { marketType, marketMessage, betMessage } = content;


          let overtimeLink;
          overtimeLink = "https://www.overtimemarkets.xyz/tickets/";
          let linkTransaction = "https://optimistic.etherscan.io/address/";
          let embed;
          if(isSystem) {
            embed = {
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
                      "](https://www.overtimemarkets.xyz/markets/" +
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
                  name: ":coin: System:",
                  value: mustWins
                },
                {
                  name: ":link: Ticket address:",
                  value:
                      "[" +
                      overtimeMarketTrade.id +
                      "](" + overtimeLink +
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
                  value: buyIn + " " + moneySymbol + buyInAmountUSD
                },
                {
                  name: ":coin: Payout:",
                  value: roundTo2Decimals(buyIn * odds) + " " + moneySymbol + payoutInUSD,
                },
                {
                  name: ":coin: Fees:",
                  value: formatV2Amount(overtimeMarketTrade.fees, overtimeMarketTrade.collateral).toFixed(2) + " " + moneySymbol,
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
          } else {
            embed = {
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
                      "](https://www.overtimemarkets.xyz/markets/" +
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
                      "](" + overtimeLink +
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
                  value: buyIn + " " + moneySymbol + buyInAmountUSD
                },
                {
                  name: ":coin: Payout:",
                  value: roundTo2Decimals(buyIn * odds) + " " + moneySymbol + payoutInUSD,
                },
                {
                  name: ":coin: Fees:",
                  value: formatV2Amount(overtimeMarketTrade.fees, overtimeMarketTrade.collateral).toFixed(2) + " " + moneySymbol,
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
          }
          let overtimeTradesChannel;
          let additionalText;
          if(Number(multiplier*amountInCurrency)<500){
            if(payoutInCurrency>=1000){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_OPT_BIG_PAYOUT);
            } else if(overtimeMarketTrade.isLive){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_OPT_LIVE_SMALL);
            } else {
             overtimeTradesChannel = await clientNewListings.channels
                .fetch(CHANNEL_OPT_SMALL);
            }
          } else{
            if(overtimeMarketTrade.isLive){
             overtimeTradesChannel = await clientNewListings.channels
                .fetch(CHANNEL_OPT_LIVE_LARGE);
              additionalText = "OP: LIVE TRADE";
          } else {
             overtimeTradesChannel = await clientNewListings.channels
                .fetch(CHANNEL_OPT_LARGE);
              additionalText = "OP: Normal TRADE";
            }
          }
          if(isSGP){
            embed.fields.push(
                {
                  name: ":coin: SGP:",
                  value: isSGP,
                }
            );
          }

          if(smartTicketOwner){
            embed.fields.push(
                {
                  name: ":coin: Smart ticket owner:",
                  value: smartTicketOwner,
                }
            );
          }

          await sendMessageIfNotDuplicate(overtimeTradesChannel, embed, overtimeMarketTrade.id, additionalText,mollyChannel);
          console.log("#@#@#@Sending V2 message: "+JSON.stringify(embed));
          writenOvertimeV2Trades.push(overtimeMarketTrade.id);
          redisClient.lpush(overtimeV2TradesKey, overtimeMarketTrade.id);

        } else {
          let parlayMessage;
          try {
            parlayMessage = await getV2ParlayMessage(overtimeMarketTrade, "", typeMap);
          } catch (err) {
            console.warn(`⚠️ OP Skipping trade ${overtimeMarketTrade?.id} in getV2ParlayMessage:`, err?.message || err);
            continue; // skip this trade
          }
          if (!parlayMessage) {
            console.warn(`⚠️ OP Skipping trade ${overtimeMarketTrade?.id} — empty parlayMessage`);
            continue;
          }

          let overTimeLink;
          overTimeLink = "https://www.overtimemarkets.xyz/tickets/"

          let linkTransaction = "https://optimistic.etherscan.io/address/";
        let  embed;
          if(isSystem) {
            embed = {
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
                      "](https://www.overtimemarkets.xyz/markets/" +
                      overtimeMarketTrade.marketsData[0].gameId +
                      ")",
                },
                {
                  name: ":link: Ticket address:",
                  value:
                      "[" +
                      overtimeMarketTrade.id +
                      "](" + overTimeLink +
                      overtimeMarketTrade.id +
                      ")",
                }, {
                  name: ":link: Ticket owner:",
                  value:
                      "[" +
                      ticketOwner +
                      "](" + linkTransaction +
                      ticketOwner +
                      ")",
                },
                {
                  name: ":coin: System:",
                  value: mustWins
                },
                {
                  name: ":coin: Buy in Amount:",
                  value: buyIn + " " + moneySymbol + buyInAmountUSD
                },
                {
                  name: ":coin: Payout:",
                  value: roundTo2Decimals(buyIn * odds) + " " + moneySymbol + payoutInUSD,
                },
                {
                  name: ":coin: Fees:",
                  value: formatV2Amount(overtimeMarketTrade.fees, overtimeMarketTrade.collateral).toFixed(2) + " " + moneySymbol,
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
          } else {
            embed = {
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
                      "](https://www.overtimemarkets.xyz/markets/" +
                      overtimeMarketTrade.marketsData[0].gameId +
                      ")",
                },
                {
                  name: ":link: Ticket address:",
                  value:
                      "[" +
                      overtimeMarketTrade.id +
                      "](" + overTimeLink +
                      overtimeMarketTrade.id +
                      ")",
                }, {
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
                  value: buyIn + " " + moneySymbol + buyInAmountUSD
                },
                {
                  name: ":coin: Payout:",
                  value: roundTo2Decimals(buyIn * odds) + " " + moneySymbol + payoutInUSD,
                },
                {
                  name: ":coin: Fees:",
                  value: formatV2Amount(overtimeMarketTrade.fees, overtimeMarketTrade.collateral).toFixed(2) + " " + moneySymbol,
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
          }
          let overtimeTradesChannel;
          let additionalText;
          if(Number(multiplier*amountInCurrency)<500){
            if(payoutInCurrency>=1000){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_OPT_BIG_PAYOUT);
            } else if(overtimeMarketTrade.isLive){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_OPT_LIVE_SMALL);
            } else {
             overtimeTradesChannel = await clientNewListings.channels
                .fetch(CHANNEL_OPT_SMALL);
            }} else{
            if(overtimeMarketTrade.isLive){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_OPT_LIVE_LARGE);
              additionalText = "OP: LIVE TRADE";
            } else {
             overtimeTradesChannel = await clientNewListings.channels
                .fetch(CHANNEL_OPT_LARGE);
              additionalText = "OP: NORMAL TRADE";
          }}
          if(isSGP){
            embed.fields.push(
                {
                  name: ":coin: SGP:",
                  value: isSGP,
                }
            );
          }

          if(smartTicketOwner){
            embed.fields.push(
                {
                  name: ":coin: Smart ticket owner:",
                  value: smartTicketOwner,
                }
            );
          }

          await sendMessageIfNotDuplicate(overtimeTradesChannel, embed, overtimeMarketTrade.id,additionalText,mollyChannel);
          console.log("#@#@#@Sending V2 message: "+JSON.stringify(embed));
          writenOvertimeV2Trades.push(overtimeMarketTrade.id);
          redisClient.lpush(overtimeV2TradesKey, overtimeMarketTrade.id);
        }


      } catch (e) {
        console.log("There was a problem while getting overtime V2 trades",e);
      }
    }
  }
}






let thalesPrice = 0.11;
let ethPrice = 3000;
let bitcoinPrice = 89000;
let circulatingSupplyOver = 69420000;
let marketCapOver = 9326707;

function getNumberLabelDecimalsMil(labelValue) {

  // Nine Zeroes for Billions
  return Math.abs(Number(labelValue)) >= 1.0e+9

      ? Math.round(Math.abs(Number(labelValue)) / 1.0e+9) + "B"
      // Six Zeroes for Millions
      : Math.abs(Number(labelValue)) >= 1.0e+6

          ? Math.round((Math.abs(Number(labelValue)) / 1.0e+6) * 100000) / 100000 + "M"
          // Three Zeroes for Thousands
          : Math.abs(Number(labelValue)) >= 1.0e+3

              ? Math.round(Math.abs(Number(labelValue)) / 1.0e+3) + "K"

              : Math.abs(Number(labelValue));

}

async function updateCirculatingAndMarketCap() {
  console.log("Updating circulating: " + clientOVERCirculating + " and Thales price: " + thalesPrice);

  if(circulatingSupplyOver && marketCapOver && clientOVERCirculating && clientOVERCirculating.user) {
  for (const [guildId, guild] of clientOVERCirculating.guilds.cache) {
    try {
      const member = await guild.members.fetch(clientOVERCirculating.user.id);
      await member.setNickname(getNumberLabelDecimalsMil(marketCapOver) + " $ Market Cap");
    } catch (e) {
      console.error("Nickname update error in guild:", guildId, e.message);
    }
  }

  clientOVERCirculating.user.setActivity(getNumberLabelDecimalsMil(circulatingSupplyOver) + " Circulating", { type: ActivityType.Watching }); // 3 = WATCHING
}}


async function updateTokenPrice() {

  let dataThales = await CoinGeckoClient.coins.fetch("overtime");
  if(dataThales.data && dataThales.data.market_data) {
    thalesPrice =   dataThales.data.market_data.current_price.usd;
    circulatingSupplyOver = dataThales.data.market_data.circulating_supply;
    marketCapOver = dataThales.data.market_data.market_cap.usd;
  }

  let dataETH = await CoinGeckoClient.coins.fetch("ethereum");
  if(dataETH.data && dataETH.data.market_data) {
    ethPrice =   dataETH.data.market_data.current_price.usd;
  }
  let dataBTC = await CoinGeckoClient.coins.fetch("bitcoin");
  if(dataBTC.data && dataBTC.data.market_data) {
    bitcoinPrice  =  dataBTC.data.market_data.current_price.usd;
  }
}


const WETH_ADDRESS_ARB = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1";

const THALES_ADDRESS_ARB = "0xE85B662Fe97e8562f4099d8A1d5A92D4B453bF30";

const OVER_ADDRESS_ARB = "0x5829d6fe7528bc8e92c4e81cc8f20a528820b51a";

const TICKET_ADDRESS_LABEL = ":link: Ticket address:";

const PAGE_SIZE = 300;
const DATA_CHUNK = 200;

async function callWithLabel(label, fn) {
  try {
    return await fn();
  } catch (e) {
    console.error(`❌ Revert on: ${label}`, e && e.message ? e.message : e);
    throw e;
  }
}

async function getAllActiveTicketsPaged(v2CoreContract) {
  const all = [];
  for (let start = 0; ; start += PAGE_SIZE) {
    const page = await callWithLabel(
        `getActiveTickets(start=${start}, size=${PAGE_SIZE})`,
        () => v2CoreContract.methods.getActiveTickets(start, PAGE_SIZE).call()
    );
    if (!page || page.length === 0) break;
    all.push(...page);
    if (page.length < PAGE_SIZE) break; // last page
  }
  return all;
}


async function robustGetTicketsData(ticketContract, ids, labelBase = 'getTicketsData') {
  const out = [];

  async function fetchChunk(arr, depth = 0) {
    if (arr.length === 0) return;

    try {
      const data = await callWithLabel(
          `${labelBase}(len=${arr.length}, depth=${depth})`,
          () => ticketContract.methods.getTicketsData(arr).call()
      );
      out.push(...data);
    } catch (err) {
      // If a single id still reverts, skip it (it went stale)
      if (arr.length === 1) {
        console.warn(`⚠️ Skipping stale/invalid ticketId: ${arr[0]}`);
        return;
      }
      // Split in half and try each half
      const mid = Math.floor(arr.length / 2);
      const left = arr.slice(0, mid);
      const right = arr.slice(mid);
      await fetchChunk(left, depth + 1);
      await fetchChunk(right, depth + 1);
    }
  }

  await fetchChunk(ids, 0);
  return out;
}



async function getOvertimeV2ARBTrades(){

  if (!typeInfoMap) return;
  const activeTickets = await getAllActiveTicketsPaged(v2ARBContract);
  let overtimeTrades = await robustGetTicketsData(v2ARBTicketContract, activeTickets,'ARB:getTicketsData');
  const typeMap = new Map(Object.entries(JSON.parse(JSON.stringify(typeInfoMap))));
  var startdate = new Date();
  var durationInMinutes = 30;
  startdate.setMinutes(startdate.getMinutes() - durationInMinutes);
  let startDateUnixTime = Math.floor(startdate.getTime());
  console.log("##### length before is "+overtimeTrades.length);
  overtimeTrades = overtimeTrades.filter(item => {
    const isNewNormal =
        startDateUnixTime < Number(item.createdAt * 1000) &&
        !writenOvertimeV2ARBTrades.includes(item.id);

    return isNewNormal ;
  });console.log("##### length is "+overtimeTrades.length);
  let overtimeTradesUQ = await overtimeTrades.filter((value, index, self) =>
          index === self.findIndex((t) => (
              t.id == value.id || t.createdAt == value.createdAt
          ))
  )
  console.log("##### length is after dupl "+overtimeTradesUQ.length);
  for (const overtimeMarketTrade of overtimeTradesUQ) {
    if ((startDateUnixTime < Number(overtimeMarketTrade.createdAt * 1000) && !writenOvertimeV2ARBTrades.includes(overtimeMarketTrade.id))) {
      try {
        if (
            !overtimeMarketTrade ||
            !Array.isArray(overtimeMarketTrade.marketsData) ||
            overtimeMarketTrade.marketsData.length === 0
        ) {
          console.warn(`⚠️ ARB Skipping trade ${overtimeMarketTrade?.id} — no marketsData`);
          continue; // skip to the next trade
        }
        const address = Web3.utils.toChecksumAddress(overtimeMarketTrade.ticketOwner);
        let isSmart =  await isSmartContract(web3ARB, address);
        let smartTicketOwner;
        if (isSmart){
          smartTicketOwner = await getOwnerOfSmartAccount(esoARBContract,overtimeMarketTrade.ticketOwner);
        }

        let mollyChannel ;
        if(MOLLY_BETS.toLowerCase() == overtimeMarketTrade.ticketOwner.toLowerCase()){
          mollyChannel = await clientNewListings.channels
              .fetch(MOLLY_CHANNEL_ID_ARB);
          console.log("##### Molly bet detected for ticket "+overtimeMarketTrade.id);
        }

        let isSystem = overtimeMarketTrade.isSystem;
        let mustWins;
        if(isSystem){
          mustWins = Number(overtimeMarketTrade.systemBetDenominator)+"/"+overtimeMarketTrade.marketsData.length;
        }else {
          mustWins = overtimeMarketTrade.marketsData.length+"/"+overtimeMarketTrade.marketsData.length;
        }


        let moneySymbol;
        let multiplier = 1;
        if (overtimeMarketTrade.collateral== WETH_ADDRESS_ARB){
          moneySymbol = "weth";
          multiplier = ethPrice;
        } else if (overtimeMarketTrade.collateral== THALES_ADDRESS_ARB){
          moneySymbol = "OVER";
          multiplier = thalesPrice;
        } else if (overtimeMarketTrade.collateral.toLowerCase()== OVER_ADDRESS_ARB.toLowerCase()){
          moneySymbol = "OVER";
          multiplier = thalesPrice;
        } else if (overtimeMarketTrade.collateral== BITCOIN_ADDRESS_ARB){
          moneySymbol = "wBTC";
          multiplier = bitcoinPrice;
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
        let odds = oddslib.from('impliedProbability', overtimeMarketTrade.totalQuote / 1e18).decimalValue.toFixed(3);
        let amountInCurrency = formatV2ARBAmount(overtimeMarketTrade.buyInAmount , overtimeMarketTrade.collateral);
        let payoutInCurrency = multiplier * roundTo2Decimals(buyIn * odds);
        let buyInAmountUSD = "";
        let payoutInUSD = "";
        if (multiplier !== 1) {
          buyInAmountUSD = " ("+roundTo2Decimals(amountInCurrency * multiplier) + " $)";
          payoutInUSD = " ("+roundTo2Decimals(payoutInCurrency) + " $)";
        }
        let isSGP = await v2ARBContract.methods.isSGPTicket(overtimeMarketTrade.id).call();
        if (overtimeMarketTrade.marketsData.length==1) {
          let content;
try {
  content = await getV2MessageContent(overtimeMarketTrade, typeMap);
} catch (err) {
  console.warn(`⚠️ ARB Skipping trade ${overtimeMarketTrade?.id} in getV2MessageContent:`, err?.message || err);
  continue; // skip this trade
}
if (!content) {
  console.warn(`⚠️ ARB Skipping trade ${overtimeMarketTrade?.id} — empty content from getV2MessageContent`);
  continue;
}
const { marketType, marketMessage, betMessage } = content;


          let overtimeLink;
          overtimeLink = "https://www.overtimemarkets.xyz/tickets/";
          let linkTransaction = "https://arbiscan.io/address/";
          let embed;
          if(isSystem){
           embed = {
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
                    "](https://www.overtimemarkets.xyz/markets/" +
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
                name: TICKET_ADDRESS_LABEL,
                value:
                    "[" +
                    overtimeMarketTrade.id +
                    "](" + overtimeLink +
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
                name: ":coin: System:",
                value: mustWins
              },
              {
                name: ":coin: Buy in Amount:",
                value: buyIn + " " + moneySymbol + buyInAmountUSD
              },
              {
                name: ":coin: Payout:",
                value: roundTo2Decimals(buyIn * odds) + " "+ moneySymbol + payoutInUSD ,
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
          } else {

            embed = {
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
                      "](https://www.overtimemarkets.xyz/markets/" +
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
                  name: TICKET_ADDRESS_LABEL,
                  value:
                      "[" +
                      overtimeMarketTrade.id +
                      "](" + overtimeLink +
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
                  value: buyIn + " " + moneySymbol + buyInAmountUSD
                },
                {
                  name: ":coin: Payout:",
                  value: roundTo2Decimals(buyIn * odds) + " "+ moneySymbol + payoutInUSD ,
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

          }

         let overtimeTradesChannel;
          let additionalText;
          if(Number(multiplier*amountInCurrency)<500){
            if(payoutInCurrency>=1000){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_ARB_BIG_PAYOUT);
            } else if(overtimeMarketTrade.isLive){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_ARB_LIVE_SMALL);

            } else {
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_ARB_SMALL);
             }
          } else{
            if(overtimeMarketTrade.isLive){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_ARB_LIVE_LARGE);
              additionalText = "ARB: LIVE TRADE";
            } else {
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_ARB_LARGE);
              additionalText = "ARB: NORMAL TRADE";
               }
          }
          if(!writenOvertimeV2ARBTrades.includes(overtimeMarketTrade.id)){
          writenOvertimeV2ARBTrades.push(overtimeMarketTrade.id);
          let newVar1 = await redisClient.lpush(overtimeV2ARBTradesKey, overtimeMarketTrade.id);
            if(isSGP){
              embed.fields.push(
                  {
                    name: ":coin: SGP:",
                    value: isSGP,
                  }
              );
            }

            if(smartTicketOwner){
              embed.fields.push(
                  {
                    name: ":coin: Smart ticket owner:",
                    value: smartTicketOwner,
                  }
              );
            }

            await sendMessageIfNotDuplicate(overtimeTradesChannel, embed, overtimeMarketTrade.id,additionalText,mollyChannel)
          console.log("#@#@#@Sending arb message: "+JSON.stringify(embed));
          }
        } else {
          let parlayMessage;
          try {
            parlayMessage = await getV2ParlayMessage(overtimeMarketTrade, "", typeMap);
          } catch (err) {
            console.warn(`⚠️ ARB Skipping trade ${overtimeMarketTrade?.id} in getV2ParlayMessage:`, err?.message || err);
            continue; // skip this trade
          }
          if (!parlayMessage) {
            console.warn(`⚠️ ARB Skipping trade ${overtimeMarketTrade?.id} — empty parlayMessage`);
            continue;
          }

          let overtimeLink;
          overtimeLink = "https://www.overtimemarkets.xyz/tickets/"
          let linkTransaction = "https://arbiscan.io/address/";
          let embed;
          if(isSystem){
           embed = {
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
                    "](https://www.overtimemarkets.xyz/markets/" +
                    overtimeMarketTrade.marketsData[0].gameId +
                    ")",
              },
              {
                name: TICKET_ADDRESS_LABEL,
                value:
                    "[" +
                    overtimeMarketTrade.id +
                    "](" + overtimeLink +
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
                value: buyIn + " " + moneySymbol + buyInAmountUSD
              },
              {
                name: ":coin: System:",
                value: mustWins
              },
              {
                name: ":coin: Payout:",
                value: roundTo2Decimals(buyIn * odds) + " "+ moneySymbol + payoutInUSD ,
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
          } else {
            embed = {
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
                      "](https://www.overtimemarkets.xyz/markets/" +
                      overtimeMarketTrade.marketsData[0].gameId +
                      ")",
                },
                {
                  name: TICKET_ADDRESS_LABEL,
                  value:
                      "[" +
                      overtimeMarketTrade.id +
                      "](" + overtimeLink +
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
                  value: buyIn + " " + moneySymbol + buyInAmountUSD
                },
                {
                  name: ":coin: Payout:",
                  value: roundTo2Decimals(buyIn * odds) + " "+ moneySymbol + payoutInUSD ,
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
          }
          let overtimeTradesChannel;
          let additionalText;
          if(Number(multiplier*amountInCurrency)<500){
            if(payoutInCurrency>=1000){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_ARB_BIG_PAYOUT);
            } else if(overtimeMarketTrade.isLive){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_ARB_LIVE_SMALL);
            } else {
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_ARB_SMALL);
            }
          } else{
            if(overtimeMarketTrade.isLive){
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_ARB_LIVE_LARGE);
              additionalText = "ARB: LIVE TRADE";
            } else {
               overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_ARB_LARGE);
               additionalText = "ARB: NORMAL TRADE";
               }}


          if(!writenOvertimeV2ARBTrades.includes(overtimeMarketTrade.id)) {
            writenOvertimeV2ARBTrades.push(overtimeMarketTrade.id);
            let lpush = await redisClient.lpush(overtimeV2ARBTradesKey, overtimeMarketTrade.id);
            if(isSGP){
              embed.fields.push(
                  {
                    name: ":coin: SGP:",
                    value: isSGP,
                  }
              );
            }

            if(smartTicketOwner){
              embed.fields.push(
                  {
                    name: ":coin: Smart ticket owner:",
                    value: smartTicketOwner,
                  }
              );
            }

            await sendMessageIfNotDuplicate(overtimeTradesChannel, embed, overtimeMarketTrade.id,additionalText,mollyChannel)
            console.log("#@#@#@Sending arb message: "+JSON.stringify(embed));
          }
        }


      } catch (e) {
        console.log("There was a problem while getting overtime V2 trades arb #@#@#@@#@",e);
      }
    }
  }
}


async function isSmartContract(web3, address) {
  try {

    const code = await web3.eth.getCode(address);
    return code && code !== '0x';
  } catch (error) {
    console.error('Error checking if address is a smart contract:', error);
    return false;
  }
}

async function getOwnerOfSmartAccount(contract, ticketOwner) {
  try {
    const owner = await contract.methods.getOwner(ticketOwner).call();
    return owner;
  } catch (err) {
    console.error("Error calling getOwner():", err);
    return null;
  }
}


const OVER_ADDRESS_BASE = "0x7750c092e284e2c7366f50c8306f43c7eb2e82a2";

async function getOvertimeV2BASETrades(){

  if (!typeInfoMap) return;
  const activeTickets = await getAllActiveTicketsPaged(v2BASEContract);
  let overtimeTrades = await robustGetTicketsData(v2BASETicketContract, activeTickets,'BASE:getTicketsData');
  const typeMap = new Map(Object.entries(JSON.parse(JSON.stringify(typeInfoMap))));
  var startdate = new Date();
  var durationInMinutes = 30;
  startdate.setMinutes(startdate.getMinutes() - durationInMinutes);
  let startDateUnixTime = Math.floor(startdate.getTime());
  console.log("##### length before is "+overtimeTrades.length);
  overtimeTrades = overtimeTrades.filter(item => {
    const isNewNormal =
        startDateUnixTime < Number(item.createdAt * 1000) &&
        !writenOvertimeV2BASETrades.includes(item.id);


    return isNewNormal;
  });console.log("##### length is "+overtimeTrades.length);
  let overtimeTradesUQ = await overtimeTrades.filter((value, index, self) =>
          index === self.findIndex((t) => (
              t.id == value.id || t.createdAt == value.createdAt
          ))
  )
  console.log("##### length is after dupl "+overtimeTradesUQ.length);
  for (const overtimeMarketTrade of overtimeTradesUQ) {
    if (  (startDateUnixTime < Number(overtimeMarketTrade.createdAt * 1000) && !writenOvertimeV2BASETrades.includes(overtimeMarketTrade.id))) {
      try {
        if (
            !overtimeMarketTrade ||
            !Array.isArray(overtimeMarketTrade.marketsData) ||
            overtimeMarketTrade.marketsData.length === 0
        ) {
          console.warn(`⚠️ Skipping trade ${overtimeMarketTrade?.id} — no marketsData`);
          continue; // skip to the next trade
        }
        const address = Web3.utils.toChecksumAddress(overtimeMarketTrade.ticketOwner);
        let isSmart =  await isSmartContract(web3BASE, address);
        let smartTicketOwner;
        if (isSmart){
          smartTicketOwner = await getOwnerOfSmartAccount(esoBASEContract,overtimeMarketTrade.ticketOwner);
        }
        let isSystem = overtimeMarketTrade.isSystem;
        let mustWins;
        if(isSystem){
          mustWins = Number(overtimeMarketTrade.systemBetDenominator)+"/"+overtimeMarketTrade.marketsData.length;
        }else {
          mustWins = overtimeMarketTrade.marketsData.length+"/"+overtimeMarketTrade.marketsData.length;
        }

        let mollyChannel ;
        if(MOLLY_BETS.toLowerCase() == overtimeMarketTrade.ticketOwner.toLowerCase()){
          mollyChannel  = await clientNewListings.channels
              .fetch(MOLLY_CHANNEL_ID_BASE);
          console.log("##### Molly bet detected for ticket "+overtimeMarketTrade.id);
        }

        let moneySymbol;
        let multiplier = 1;

        if (overtimeMarketTrade.collateral== WETH_ADDRESS_BASE){
          moneySymbol = "weth";
          multiplier = ethPrice;
        } else if (overtimeMarketTrade.collateral== THALES_ADDRESS_BASE){
          moneySymbol = "OVER";
          multiplier = thalesPrice;
        } else if (overtimeMarketTrade.collateral.toLowerCase()== OVER_ADDRESS_BASE.toLowerCase()){
          moneySymbol = "OVER";
          multiplier = thalesPrice;
        } else if (overtimeMarketTrade.collateral== BITCOIN_ADDRESS_BASE){
          moneySymbol = "cbBTC";
          multiplier = bitcoinPrice;
        } else {
          moneySymbol = "USDC";
        }

        let ticketOwner;
        if(overtimeMarketTrade.ticketOwner == STAKING_BASE ){
          ticketOwner =  await v2StakingBASE.methods.ticketToUser(overtimeMarketTrade.id).call();
        } else if (overtimeMarketTrade.ticketOwner == FREE_BET_BASE) {
          ticketOwner =  await v2FreeBetgBASE.methods.ticketToUser(overtimeMarketTrade.id).call();
        } else
        {
          ticketOwner = overtimeMarketTrade.ticketOwner;
        }

        let buyIn = roundTo2Decimals(formatV2BASEAmount(overtimeMarketTrade.buyInAmount , overtimeMarketTrade.collateral));
        let odds = oddslib.from('impliedProbability', overtimeMarketTrade.totalQuote / 1e18).decimalValue.toFixed(3);
        let amountInCurrency = formatV2BASEAmount(overtimeMarketTrade.buyInAmount , overtimeMarketTrade.collateral);
        let payoutInCurrency = multiplier * roundTo2Decimals(buyIn * odds);
        let buyInAmountUSD = "";
        let payoutInUSD = "";
        if (multiplier !== 1) {
          buyInAmountUSD = " ("+roundTo2Decimals(amountInCurrency * multiplier) + " $)";
          payoutInUSD = " ("+roundTo2Decimals(payoutInCurrency) + " $)";
        }

        let isSGP = await v2BASEContract.methods.isSGPTicket(overtimeMarketTrade.id).call();
        if (overtimeMarketTrade.marketsData.length==1) {
          let content;
try {
  content = await getV2MessageContent(overtimeMarketTrade, typeMap);
} catch (err) {
  console.warn(`⚠️ Skipping trade ${overtimeMarketTrade?.id} in getV2MessageContent:`, err?.message || err);
  continue; // skip this trade
}
if (!content) {
  console.warn(`⚠️ Skipping trade ${overtimeMarketTrade?.id} — empty content from getV2MessageContent`);
  continue;
}
const { marketType, marketMessage, betMessage } = content;


          let overtimeLink;
          overtimeLink = "https://www.overtimemarkets.xyz/tickets/";
          let linkTransaction = "https://basescan.org/address/";
          let embed;
          if(isSystem){
            embed = {
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
                      "](https://www.overtimemarkets.xyz/markets/" +
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
                  name: TICKET_ADDRESS_LABEL,
                  value:
                      "[" +
                      overtimeMarketTrade.id +
                      "](" + overtimeLink +
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
                  name: ":coin: System:",
                  value: mustWins
                },
                {
                  name: ":coin: Buy in Amount:",
                  value: buyIn + " " + moneySymbol + buyInAmountUSD
                },
                {
                  name: ":coin: Payout:",
                  value: roundTo2Decimals(buyIn * odds) + " "+ moneySymbol + payoutInUSD ,
                },
                {
                  name: ":coin: Fees:",
                  value: formatV2BASEAmount(overtimeMarketTrade.fees , overtimeMarketTrade.collateral).toFixed(2) + " " + moneySymbol ,
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
          } else {

            embed = {
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
                      "](https://www.overtimemarkets.xyz/markets/" +
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
                  name: TICKET_ADDRESS_LABEL,
                  value:
                      "[" +
                      overtimeMarketTrade.id +
                      "](" + overtimeLink +
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
                  value: buyIn + " " + moneySymbol + buyInAmountUSD
                },
                {
                  name: ":coin: Payout:",
                  value: roundTo2Decimals(buyIn * odds) + " "+ moneySymbol + payoutInUSD ,
                },
                {
                  name: ":coin: Fees:",
                  value: formatV2BASEAmount(overtimeMarketTrade.fees , overtimeMarketTrade.collateral).toFixed(2) + " " + moneySymbol ,
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

          }

          let overtimeTradesChannel;
          let additionalText;
          if(Number(multiplier*amountInCurrency)<500){
            if(payoutInCurrency>=1000){
              overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_BASE_BIG_PAYOUT);
            } else if(overtimeMarketTrade.isLive){
              overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_BASE_LIVE_SMALL);

            } else {
              overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_BASE_SMALL);
            }
          } else{
            if(overtimeMarketTrade.isLive){
              overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_BASE_LIVE_LARGE);
              additionalText = "BASE: LIVE TRADE";

            } else {
              overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_BASE_LARGE);
              additionalText = "BASE: NORMAL TRADE";
            }
          }
          if(!writenOvertimeV2BASETrades.includes(overtimeMarketTrade.id)){
            writenOvertimeV2BASETrades.push(overtimeMarketTrade.id);
            let newVar1 = await redisClient.lpush(overtimeV2BASETradesKey, overtimeMarketTrade.id);
            if(isSGP){
              embed.fields.push(
                  {
                    name: ":coin: SGP:",
                    value: isSGP,
                  }
              );
            }

            if(smartTicketOwner){
              embed.fields.push(
                  {
                    name: ":coin: Smart ticket owner:",
                    value: smartTicketOwner,
                  }
              );
            }

            await sendMessageIfNotDuplicate(overtimeTradesChannel, embed, overtimeMarketTrade.id,additionalText,mollyChannel)
            console.log("#@#@#@Sending base message: "+JSON.stringify(embed));
          }
        } else {
          let parlayMessage;
          try {
            parlayMessage = await getV2ParlayMessage(overtimeMarketTrade, "", typeMap);
          } catch (err) {
            console.warn(`⚠️ Skipping trade ${overtimeMarketTrade?.id} in getV2ParlayMessage:`, err?.message || err);
            continue; // skip this trade
          }
          if (!parlayMessage) {
            console.warn(`⚠️ Skipping trade ${overtimeMarketTrade?.id} — empty parlayMessage`);
            continue;
          }

          let overtimeLink;
          overtimeLink = "https://www.overtimemarkets.xyz/tickets/"
          let linkTransaction = "https://basescan.org/address/";
          let embed;
          if(isSystem){
            embed = {
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
                      "](https://www.overtimemarkets.xyz/markets/" +
                      overtimeMarketTrade.marketsData[0].gameId +
                      ")",
                },
                {
                  name: TICKET_ADDRESS_LABEL,
                  value:
                      "[" +
                      overtimeMarketTrade.id +
                      "](" + overtimeLink +
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
                  value: buyIn + " " + moneySymbol + buyInAmountUSD
                },
                {
                  name: ":coin: System:",
                  value: mustWins
                },
                {
                  name: ":coin: Payout:",
                  value: roundTo2Decimals(buyIn * odds) + " "+ moneySymbol + payoutInUSD ,
                },
                {
                  name: ":coin: Fees:",
                  value: formatV2BASEAmount(overtimeMarketTrade.fees , overtimeMarketTrade.collateral).toFixed(2) + " " + moneySymbol,
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
          } else {
            embed = {
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
                      "](https://www.overtimemarkets.xyz/markets/" +
                      overtimeMarketTrade.marketsData[0].gameId +
                      ")",
                },
                {
                  name: TICKET_ADDRESS_LABEL,
                  value:
                      "[" +
                      overtimeMarketTrade.id +
                      "](" + overtimeLink +
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
                  value: buyIn + " " + moneySymbol + buyInAmountUSD
                },
                {
                  name: ":coin: Payout:",
                  value: roundTo2Decimals(buyIn * odds) + " "+ moneySymbol + payoutInUSD ,
                },
                {
                  name: ":coin: Fees:",
                  value: formatV2BASEAmount(overtimeMarketTrade.fees , overtimeMarketTrade.collateral).toFixed(2) + " " + moneySymbol,
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
          }
          let overtimeTradesChannel;
          let additionalText;
          if(Number(multiplier*amountInCurrency)<500){
            if(payoutInCurrency>=1000){
              overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_BASE_BIG_PAYOUT);
            } else if(overtimeMarketTrade.isLive){
              overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_BASE_LIVE_SMALL);

            } else {
              overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_BASE_SMALL);
            }
          } else{
            if(overtimeMarketTrade.isLive){
              overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_BASE_LIVE_LARGE);
              additionalText = "BASE: LIVE TRADE";

            } else {
              overtimeTradesChannel = await clientNewListings.channels
                  .fetch(CHANNEL_BASE_LARGE);
              additionalText = "BASE: NORMAL TRADE";
            }
          }


          if(!writenOvertimeV2BASETrades.includes(overtimeMarketTrade.id)) {
            writenOvertimeV2BASETrades.push(overtimeMarketTrade.id);
            let lpush = await redisClient.lpush(overtimeV2BASETradesKey, overtimeMarketTrade.id);
            if(isSGP){
              embed.fields.push(
                  {
                    name: ":coin: SGP:",
                    value: isSGP,
                  }
              );
            }

            if(smartTicketOwner){
              embed.fields.push(
                  {
                    name: ":coin: Smart ticket owner:",
                    value: smartTicketOwner,
                  }
              );
            }

            await sendMessageIfNotDuplicate(overtimeTradesChannel, embed, overtimeMarketTrade.id,additionalText,mollyChannel)
            console.log("#@#@#@Sending base message: "+JSON.stringify(embed));
          }
        }


      } catch (e) {
        console.log("There was a problem while getting overtime V2 trades base #@#@#@@#@",e);
      }
    }
  }
}

async function cleanUpDuplicateMessages() {
  for (const channelId of ALL_CHANNEL_IDS) {
    try {
      const channel = await clientNewListings.channels.fetch(channelId);
      const messages = await channel.messages.fetch({ limit: 50 });

      const seenTickets = new Map();

      for (const [id, message] of messages) {
        if (!message.embeds || message.embeds.length === 0) continue;

        const ticketField = message.embeds
            .flatMap(embed => embed.fields || [])
            .find(field => field.name === TICKET_ADDRESS_LABEL || field.name === ":link: Ticket  address:");
        if (!ticketField || !ticketField.value) continue;

        const ticketIdMatch = ticketField.value.match(/\[([^\]]+)\]/); // extract content from [ticketId](...)
        if (!ticketIdMatch) continue;

        const ticketId = ticketIdMatch[1];

        if (seenTickets.has(ticketId)) {
          console.log(`Deleting duplicate message in channel ${channelId} for ticket: ${ticketId}`);
          await message.delete().catch(console.error);
        } else {
          seenTickets.set(ticketId, message);
        }
      }

    } catch (err) {
      console.error(`❌ Error cleaning up messages in channel ${channelId}:`, err);
    }
  }
}
