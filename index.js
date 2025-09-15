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

const { Telegraf } = require('telegraf');
const path = require('path');
const puppeteer = require('puppeteer');
const tg = new Telegraf(process.env.TG_OVERTIME_BOT_TOKEN);
const cron = require('node-cron');

const multipliersUrl = 'https://overdrop.overtime.io/game-multipliers';
const networkId = 10; // Optimism
const adminApiKey = process.env.OVER_API_KEY;
const tz = 'Etc/GMT-1';
const DISCORD_ANNOUNCEMENTS = '906495542744469544';
const DISCORD_MONEY_TIPS     = '1407664919033413705';

const TG_CHAT_ID = -1003066123689;
const TIPS_THREAD_ID = 5;
const WINNING_THREAD_ID = 17;
const LEADERBOARD_THREAD_ID = 15;

const channelDiscTGRouting = {
  [DISCORD_ANNOUNCEMENTS]: {
    chatId: TG_CHAT_ID,
    threadId: null,
    label: '📣 | announcements',
  },
  [DISCORD_MONEY_TIPS]: {
    chatId: TG_CHAT_ID,
    threadId: TIPS_THREAD_ID,
    label: '💸 | tax-free-money-tips',
  }
};

const allowedDiscordChannelIds = new Set(Object.keys(channelDiscTGRouting));



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




async function sendMessageIfNotDuplicate(channel, embed, uniqueValue, additionalText,mollyChannel,bigWinChannel,bigWinTicketURL) {
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
  if(bigWinChannel){
    bigWinChannel.send({ embeds: [embed] }).catch(console.error);
    await captureTicketAndSendToTelegram(bigWinTicketURL, { tg, TG_CHAT_ID, WINNING_THREAD_ID, filename: 'winning-ticket.png' });
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

    const dmEmbed = { ...embed, fields: [...embed.fields] };

    if (dmEmbed.fields && dmEmbed.fields.length > 0) {
      dmEmbed.fields[0] = {
        ...dmEmbed.fields[0],
        value: dmEmbed.fields[0].value + (" "+additionalText)
      };
    }

    await userLuka.send({ embeds: [dmEmbed] });
    await userDuka.send({ embeds: [dmEmbed] });
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

        const address = Web3.utils.toChecksumAddress(overtimeMarketTrade.ticketOwner);
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
        let isBigWinChannel;
        if(overtimeMarketTrade.isUserTheWinner && (payoutInCurrency>=5000 || odds>=5)){
            isBigWinChannel = await clientNewListings.channels
                .fetch(CHANNEL_OP_BIG_WIN);

            console.log("##### Big win detected for ticket "+overtimeMarketTrade.id);
        }


        let isSGP = await v2Contract.methods.isSGPTicket(overtimeMarketTrade.id).call();
        if (overtimeMarketTrade.marketsData.length==1) {
          let {marketType, marketMessage, betMessage} = await getV2MessageContent(overtimeMarketTrade,typeMap);

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

          await sendMessageIfNotDuplicate(overtimeTradesChannel, embed, overtimeMarketTrade.id, additionalText,mollyChannel,isBigWinChannel);
          writenOvertimeV2Trades.push(overtimeMarketTrade.id);
          redisClient.lpush(overtimeV2TradesKey, overtimeMarketTrade.id);

        } else {
          let parlayMessage = "";
          parlayMessage = await getV2ParlayMessage(overtimeMarketTrade, parlayMessage,typeMap);
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

          await sendMessageIfNotDuplicate(overtimeTradesChannel, embed, overtimeMarketTrade.id,additionalText,mollyChannel,isBigWinChannel,overtimeMarketTrade.id);

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
  overtimeTrades = await overtimeTrades.filter(item => !writenOvertimeV2ARBTrades.includes(item.id) && startDateUnixTime < Number(item.createdAt * 1000));
  console.log("##### length is "+overtimeTrades.length);
  let overtimeTradesUQ = await overtimeTrades.filter((value, index, self) =>
          index === self.findIndex((t) => (
              t.id == value.id || t.createdAt == value.createdAt
          ))
  )
  console.log("##### length is after dupl "+overtimeTradesUQ.length);
  for (const overtimeMarketTrade of overtimeTradesUQ) {
    if (startDateUnixTime < Number(overtimeMarketTrade.createdAt * 1000) && !writenOvertimeV2ARBTrades.includes(overtimeMarketTrade.id)) {
      try {

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
        let isBigWinChannel;
        if(overtimeMarketTrade.isUserTheWinner && (payoutInCurrency>=5000 || odds>=5)){
          isBigWinChannel = await clientNewListings.channels
              .fetch(CHANNEL_ARB_BIG_WIN);
          console.log("##### Big win detected for ticket "+overtimeMarketTrade.id);
        }
        let isSGP = await v2ARBContract.methods.isSGPTicket(overtimeMarketTrade.id).call();
        if (overtimeMarketTrade.marketsData.length==1) {
          let {marketType, marketMessage, betMessage} = await getV2MessageContent(overtimeMarketTrade,typeMap);

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

            await sendMessageIfNotDuplicate(overtimeTradesChannel, embed, overtimeMarketTrade.id,additionalText,mollyChannel,isBigWinChannel,overtimeMarketTrade.id)
          console.log("#@#@#@Sending arb message: "+JSON.stringify(embed));
          }
        } else {
          let parlayMessage = "";
          parlayMessage = await getV2ParlayMessage(overtimeMarketTrade, parlayMessage,typeMap);
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

            await sendMessageIfNotDuplicate(overtimeTradesChannel, embed, overtimeMarketTrade.id,additionalText,mollyChannel,isBigWinChannel,overtimeMarketTrade.id)
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
  overtimeTrades = await overtimeTrades.filter(item => !writenOvertimeV2BASETrades.includes(item.id) && startDateUnixTime < Number(item.createdAt * 1000));
  console.log("##### length is "+overtimeTrades.length);
  let overtimeTradesUQ = await overtimeTrades.filter((value, index, self) =>
          index === self.findIndex((t) => (
              t.id == value.id || t.createdAt == value.createdAt
          ))
  )
  console.log("##### length is after dupl "+overtimeTradesUQ.length);
  for (const overtimeMarketTrade of overtimeTradesUQ) {
    if (startDateUnixTime < Number(overtimeMarketTrade.createdAt * 1000) && !writenOvertimeV2BASETrades.includes(overtimeMarketTrade.id)) {
      try {

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
        let isBigWinChannel;
        if(overtimeMarketTrade.isUserTheWinner && (payoutInCurrency>=5000 || odds>=5)){
          isBigWinChannel = await clientNewListings.channels
              .fetch(CHANNEL_BASE_BIG_WIN);
          console.log("##### Big win detected for ticket "+overtimeMarketTrade.id);
        }
        let isSGP = await v2BASEContract.methods.isSGPTicket(overtimeMarketTrade.id).call();
        if (overtimeMarketTrade.marketsData.length==1) {
          let {marketType, marketMessage, betMessage} = await getV2MessageContent(overtimeMarketTrade,typeMap);

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

            await sendMessageIfNotDuplicate(overtimeTradesChannel, embed, overtimeMarketTrade.id,additionalText,mollyChannel,isBigWinChannel,overtimeMarketTrade.id)
            console.log("#@#@#@Sending base message: "+JSON.stringify(embed));
          }
        } else {
          let parlayMessage = "";
          parlayMessage = await getV2ParlayMessage(overtimeMarketTrade, parlayMessage,typeMap);
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

            await sendMessageIfNotDuplicate(overtimeTradesChannel, embed, overtimeMarketTrade.id,additionalText,mollyChannel,isBigWinChannel,overtimeMarketTrade.id)
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

function formatDiscordMessageForTelegram(message, label) {
  const author = message.author?.username ?? 'Unknown';
  const content = cleanDiscordContent((message.content || '').trim());

  const embedText = (message.embeds || [])
      .map(e => {
        const title = e.title ? `\n*${cleanDiscordContent(e.title)}*` : '';
        const desc  = e.description ? `\n${cleanDiscordContent(e.description)}` : '';
        return `${title}${desc}`;
      })
      .join('');

  const permalink = `https://discord.com/channels/${message.guild?.id}/${message.channel?.id}/${message.id}`;
  const header = label ? `*${label}*` : '*From Discord*';

  const text = `${header}\n*${author}:* ${content || '(no text)'}${embedText}\n\n[View on Discord](${permalink})`;

  return { text, parse_mode: 'Markdown' };
}



async function sendToTelegram(chatId, threadId, text, opts = {}) {
  const payload = {
    parse_mode: 'Markdown',
    disable_web_page_preview: true,
    ...opts,
  };
  // Only include message_thread_id if we actually have one
  if (typeof threadId === 'number') payload.message_thread_id = threadId;

  return tg.telegram.sendMessage(chatId, text, payload);
}

async function forwardAttachmentsToTelegram(attachments, chatId, threadId) {
  for (const att of attachments.values()) {
    const url = att.url;
    const fileName = att.name || 'file';
    const contentType = att.contentType || '';
    const isImage = contentType.startsWith('image/') || /\.(png|jpe?g|gif|webp)$/i.test(fileName);

    try {
      const { data } = await axios.get(url, { responseType: 'arraybuffer' });
      const file = { source: Buffer.from(data), filename: fileName };
      const baseOpts = {};
      if (typeof threadId === 'number') baseOpts.message_thread_id = threadId;

      if (isImage) {
        await tg.telegram.sendPhoto(chatId, file, baseOpts);
      } else {
        await tg.telegram.sendDocument(chatId, file, baseOpts);
      }
    } catch (e) {
      console.error(`Failed to forward attachment ${fileName}:`, e?.response?.status || e.message);
    }
  }
}

clientNewListings.on('messageCreate', async (message) => {
  try {
    if (!message.guild) return;           // ignore DMs
    if (message.author?.bot) return;      // ignore bots

    const route = channelDiscTGRouting[message.channel.id];
    if (!route) return;                   // only forward mapped channels

    const { chatId, threadId, label } = route;

    const { text, parse_mode } = formatDiscordMessageForTelegram(message, label);


    await sendToTelegram(chatId, threadId, text, { parse_mode });

    if (message.attachments?.size) {
      await forwardAttachmentsToTelegram(message.attachments, chatId, threadId);
    }
  } catch (err) {
    console.error('Forwarding error:', err?.response?.data?.description || err.message);
  }
});

function cleanDiscordContent(input) {
  if (!input) return '';

  return input
      // remove custom emoji tags like <:name:123456789012345678> or <a:name:123456789012345678>
      .replace(/<a?:\w+:\d+>/g, '')
      // remove Nitro-style shortcodes like :GREENCHECK: or :pepelaugh:
      .replace(/:\w+?:/g, '')
      .trim();
}

function toTicketUrl(urlOrId) {
  if (/^https?:\/\//i.test(urlOrId)) return urlOrId;
  if (/^0x[a-fA-F0-9]{8,}$/.test(urlOrId)) {
    return `https://www.overtimemarkets.xyz/tickets/${urlOrId}`;
  }
  throw new Error(`Not a valid ticket URL or id: ${urlOrId}`);
}

/**
 * Screenshot the ticket card and (optionally) send it to Telegram.
 * urlOrId:  full ticket URL or "0x..." id
 * opts:     { tg, chatId, threadId, caption, filename, viewportWidth, viewportHeight, deviceScaleFactor, timeoutMs }
 * Returns a Buffer with the PNG.
 */
async function captureTicketAndSendToTelegram(
    urlOrId,
    {
      tg,
      chatId = -1003066123689,                 // send to TG only if provided
      threadId = 17,
      caption = undefined,
      filename = 'ticket.png',
      viewportWidth = 1920,
      viewportHeight = 1080,
      deviceScaleFactor = 2,
      timeoutMs = 45000,
    } = {}
) {
  const url = toTicketUrl(urlOrId);

  const browser = await puppeteer.launch({
    headless: true, // <-- this matches your working script
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
    defaultViewport: { width: viewportWidth, height: viewportHeight, deviceScaleFactor },
  });

  try {
    const page = await browser.newPage();
    await page.setUserAgent(
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36'
    );

    // Go to ticket
    await page.goto(url, { waitUntil: 'networkidle2', timeout: timeoutMs });
    await page.waitForTimeout(1200); // small settle

    // ---- Locate the card using XPath-first strategy ----
    const handle = await locateTicketCard(page, timeoutMs);
    if (!handle) throw new Error('Ticket card element not found');

    // Ensure it’s in view
    await handle.evaluate(el => el.scrollIntoView({ block: 'center', inline: 'center', behavior: 'instant' }));
    await page.waitForTimeout(150);

    // Direct element screenshot (tight crop)
    const buffer = await handle.screenshot({ type: 'png' });

    // Optional Telegram send
    if (tg && typeof chatId === 'number') {
      const extra = {};
      if (typeof threadId === 'number') extra.message_thread_id = threadId;
      if (caption) extra.caption = caption;

      await tg.telegram.sendPhoto(
          chatId,
          { source: buffer, filename: path.basename(filename) },
          extra
      );
    }

    return buffer;
  } finally {
    await browser.close().catch(() => {});
  }
}

/**
 * Robust locator for the ticket card.
 * Primary: find the <span> that contains "ticket" (case-insensitive) and take an ancestor div.
 * Fallbacks: use "payout", "total quote", "buy-in" anchors and pick the smallest valid container.
 */
async function locateTicketCard(page, timeoutMs) {
  // Helpers
  const A2Z = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  const a2z = 'abcdefghijklmnopqrstuvwxyz';
  const lower = `translate(normalize-space(.), '${A2Z}', '${a2z}')`;

  // 1) Your known-working XPath (prefer this)
  const xpaths = [
    // climb 3 containers above the "ticket" label (tuned to current DOM)
    `//span[contains(${lower}, 'ticket')]/ancestor::div[3]`,
    // alternative climb depths in case of slight DOM shifts
    `//span[contains(${lower}, 'ticket')]/ancestor::div[2]`,
    `//span[contains(${lower}, 'ticket')]/ancestor::div[4]`,
    // anchor on "payout" and climb (currency varies, so we key on the word)
    `//*[contains(${lower}, 'payout')]/ancestor::div[6]`,
    `//*[contains(${lower}, 'total quote')]/ancestor::div[6]`,
    `//*[contains(${lower}, 'buy-in')]/ancestor::div[6]`,
  ];

  const deadline = Date.now() + Math.max(4000, Math.min(15000, timeoutMs / 3));
  let candidates = [];

  // Try each xpath, collect any matching elements
  for (const xp of xpaths) {
    try {
      const els = await page.$x(xp);
      if (els && els.length) candidates.push(...els);
    } catch (_) {}
    if (Date.now() > deadline) break;
  }

  // If nothing yet, wait briefly for the main one and retry once
  if (candidates.length === 0) {
    try {
      await page.waitForXPath(xpaths[0], { timeout: 3000 });
      const els = await page.$x(xpaths[0]);
      if (els && els.length) candidates.push(...els);
    } catch (_) {}
  }

  if (candidates.length === 0) return null;

  // Score candidates: prefer smallest area above thresholds (tight card), then closeness to viewport center
  const { width: vw, height: vh } = await page.viewport();
  let best = null;
  let bestScore = Number.POSITIVE_INFINITY;

  for (const el of candidates) {
    const box = await el.boundingBox().catch(() => null);
    if (!box) continue;

    // sanity: ignore tiny wrappers
    if (box.width < 380 || box.height < 180) continue;

    const area = box.width * box.height;
    const cx = box.x + box.width / 2;
    const cy = box.y + box.height / 2;
    const centerDist2 = (cx - vw / 2) ** 2 + (cy - vh / 2) ** 2;

    // smaller area + center bias
    const score = area * 1.0 + centerDist2 * 0.001;
    if (score < bestScore) {
      bestScore = score;
      best = el;
    }
  }

  return best;
}

const gameInfoUrl = (gameId) =>
    `https://api.overtime.io/overtime-v2/games-info/${gameId}`;

const marketBasicUrl = (marketId) =>
    `https://api.overtime.io/overtime-v2/networks/${networkId}/markets/${marketId}?onlyBasicProperties=true&adminApiKey=${encodeURIComponent(adminApiKey)}`;

const marketDeepLink = (marketId) =>
    `https://www.overtimemarkets.xyz/markets/${marketId}`;

// ===== Cache =====
/**
 * cachedUpcomingDayMarkets: Array<{
 *   marketId: string,       // same as gameId from multipliers feed
 *   multiplier: string,     // as received
 *   maturityMs: number,     // milliseconds
 *   gameName: string        // "Home - Away"
 * }>
 */
let cachedUpcomingDayMarkets = [];
const gameNameById = new Map();

// ===== Helpers =====
function parseMaturityMs(raw) {
  if (raw == null) return null;
  const n = Number(raw);
  if (Number.isNaN(n)) return null;
  // seconds vs milliseconds
  return n < 1e12 ? n * 1000 : n;
}

function fmtTimeCET(ms) {
  // CET fixed: add +1h, then format in UTC
  const d = new Date(ms + 60 * 60 * 1000);
  const hhmm = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'UTC',
    hour: '2-digit',
    minute: '2-digit'
  }).format(d);
  return `${hhmm} CET`;
}

function formatPercent(multiplier) {
  const n = Number(multiplier);
  if (Number.isNaN(n)) return `${multiplier}%`;
  const pct = n <= 1 ? n * 100 : n; // supports 0.1 or 10
  return `${Number.isInteger(pct) ? pct.toFixed(0) : pct.toFixed(2)}%`;
}

async function getGameMessage(gameId) {
  const gameResp = await axios.get(gameInfoUrl(gameId), { timeout: 15000 });
  const game = gameResp.data;
  let homeTeam, awayTeam;
  if (game.teams[0].isHome) {
    homeTeam = game.teams[0].name;
    awayTeam = game.teams[1].name;
  } else {
    awayTeam = game.teams[0].name;
    homeTeam = game.teams[1].name;
  }
  return `${homeTeam} - ${awayTeam}`;
}

// small concurrency limiter
async function mapWithLimit(items, limit, mapper) {
  const results = new Array(items.length);
  let inFlight = 0;
  let index = 0;

  return new Promise((resolve) => {
    const launch = () => {
      while (inFlight < limit && index < items.length) {
        const i = index++;
        inFlight++;
        Promise.resolve(mapper(items[i], i))
            .then((res) => { results[i] = res; })
            .catch(() => { results[i] = undefined; })
            .finally(() => {
              inFlight--;
              if (index === items.length && inFlight === 0) resolve(results);
              else launch();
            });
      }
    };
    launch();
  });
}

// Build inline keyboard (1 button per row)
function buildKeyboard(rows) {
  const maxButtons = 80; // safety cap
  const limited = rows.slice(0, maxButtons);
  const keyboard = limited.map(r => ([
    {
      text: `${r.gameName} · ${formatPercent(r.multiplier)} · ${fmtTimeCET(r.maturityMs)}`,
      url: marketDeepLink(r.marketId),
    }
  ]));
  return { reply_markup: { inline_keyboard: keyboard } };
}

function buildRowsFromCache() {
  const rows = [...cachedUpcomingDayMarkets];
  // sort by maturity then by name
  rows.sort((a, b) => (a.maturityMs - b.maturityMs) || a.gameName.localeCompare(b.gameName));
  return rows;
}

async function sendTodayXpTable(chatId, ctxForReply = null, threadId = null) {
  const rows = buildRowsFromCache();
  if (!rows.length) {
    const msg = 'No upcoming “day” XP markets at the moment.';
    if (ctxForReply) return ctxForReply.reply(msg, threadId ? { message_thread_id: threadId } : undefined);
    return tg.telegram.sendMessage(chatId, msg, threadId ? { message_thread_id: threadId } : undefined);
  }

  const header = '<b>Today’s XP Markets</b>\nTap a game to open the market:';
  const kb = buildKeyboard(rows);

  const opts = {
    parse_mode: 'HTML',
    disable_web_page_preview: true,
    ...(threadId ? { message_thread_id: threadId } : {})
  };

  if (ctxForReply) {
    await ctxForReply.reply(header, { ...kb, ...opts });
  } else {
    await tg.telegram.sendMessage(chatId, header, { ...kb, ...opts });
  }
}



async function refreshDayGamesCache() {
  try {
    if (!adminApiKey) {
      console.warn('[cache] OVERTIME_ADMIN_API_KEY not set. Skipping market fetch.');
      cachedUpcomingDayMarkets = [];
      return;
    }

    const resp = await axios.get(multipliersUrl, { timeout: 15000 });
    const all = Array.isArray(resp.data) ? resp.data : [];
    const now = Date.now();

    const dayGames = all.filter(g => String(g.type).toLowerCase() === 'day');

    const combined = await mapWithLimit(dayGames, 8, async ({ gameId, multiplier }) => {
      try {
        const [marketResp, name] = await Promise.all([
          axios.get(marketBasicUrl(gameId), { timeout: 15000 }),
          (async () => {
            if (gameNameById.has(gameId)) return gameNameById.get(gameId);
            try {
              const got = await getGameMessage(gameId);
              gameNameById.set(gameId, got);
              return got;
            } catch {
              gameNameById.set(gameId, '(unavailable)');
              return '(unavailable)';
            }
          })()
        ]);

        const market = marketResp.data || {};
        const maturityMs = parseMaturityMs(market.maturity);
        if (!maturityMs || maturityMs <= now) return undefined;

        return {
          marketId: gameId, // markets endpoint uses the same id here
          multiplier: multiplier || '—',
          maturityMs,
          gameName: name || '(unavailable)',
        };
      } catch {
        return undefined;
      }
    });

    cachedUpcomingDayMarkets = combined.filter(Boolean);
    console.log(`[cache] Upcoming “day” markets: ${cachedUpcomingDayMarkets.length}`);
  } catch (e) {
    console.error('[cache] refresh failed:', e?.response?.status, e?.message);
  }
}

// ===== Commands =====
tg.command(['today_xp', 'today-xp'], async (ctx) => {
  try {
    await sendTodayXpTable(ctx.chat.id, ctx);
  } catch (e) {
    console.error('/today_xp error:', e?.message);
    await ctx.reply('Could not prepare XP list right now.');
  }
});

tg.command('help', async (ctx) => {
  await ctx.reply('Commands:\n/today_xp — Show today’s XP markets (type: day, future maturity).');
});

// ===== Daily post at 09:00 CET (fixed) =====
cron.schedule('0 16 * * *', async () => {
  if (!TG_CHAT_ID) {
    console.warn('[cron] TELEGRAM_CHANNEL_ID not set; skipping daily post.');
    return;
  }
  try {
    await sendTodayXpTable(TG_CHAT_ID, null, LEADERBOARD_THREAD_ID);
    console.log('[cron] Posted today XP table at 09:00 CET.');
  } catch (e) {
    console.error('[cron] post failed:', e?.message);
  }
}, { timezone: tz });

// ===== Hourly cache refresh =====
setInterval(refreshDayGamesCache, 60 * 60 * 1000);

// ===== Startup =====
(async () => {
  await refreshDayGamesCache(); // warm cache once container starts
  await tg.telegram.setMyCommands([
    { command: 'today_xp', description: "Show today’s XP markets" },
    { command: 'help', description: "How to use this bot" },
  ]);
  await tg.launch();
  console.log('Telegram bot running. /today_xp enabled. Daily post at 09:00 CET. Hourly cache refresh active.');
})();