package parser;

import skadistats.clarity.processor.runner.Context;
import skadistats.clarity.processor.runner.SimpleRunner;
import skadistats.clarity.processor.stringtables.StringTables;
import skadistats.clarity.processor.stringtables.UsesStringTable;
import skadistats.clarity.io.Util;
import skadistats.clarity.model.Entity;
import skadistats.clarity.model.FieldPath;
import skadistats.clarity.model.StringTable;
import skadistats.clarity.processor.entities.Entities;
import skadistats.clarity.processor.entities.UsesEntities;
import skadistats.clarity.processor.reader.OnTickStart;
import skadistats.clarity.source.MappedFileSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class ReplayParser {
 
    public static class Team {
        public int teamId;
        public List<Player> players = new ArrayList<>();
    
        public LinkedHashMap<String, Building> buildings = new LinkedHashMap<>();
        public List<Map<String, Float>> observerWards = new ArrayList<>();
    
        public int totalCampsStacked = 0;
        public int totalRunePickups = 0;
        public int totalTowersKilled = 0;
        public int totalRoshansKilled = 0;
        public int totalSmokesUsed = 0;

        public Team(int teamId) {
            this.teamId = teamId;
        }

        public void setBuilding(String key, Building b) {
            if (b != null) {
                buildings.put(key, b);
            }
        }
    
        public Building getBuilding(String key) {
            return buildings.getOrDefault(key, null);
        }
    
        @Override
        public String toString() {
            return "Team{" +
                "teamId=" + teamId +
                ", players=" + players.size() +
                ", buildings=" + buildings +
                '}';
        }
    }

    public static class Building {
        public String name;
        public int team;
        public int health;
    
        public Building(String name, int team, int health) {
            this.name = name;
            this.team = team;
            this.health = health;
        }

        @Override
        public String toString() {
          return String.format("Building{name='%s', team=%d, health=%d}", name, team, health);
        }
    }

    public static class Player {
        public String key;
        public Integer value;
        public Integer team;
        public String name;
        public Long steamId;

        public Integer heroId;
        public Integer heroVariant;

        public Integer level;
        public Integer xp;

        public Integer networth;
        public Integer totalGold;
        public Integer currentGold;

        public Integer lifeState;
        public Integer respawnSeconds;

        public Float buybackCooldown;

        public Integer heroDamage;
        public Integer towerDamage;
        public Integer damageTakenPreReduction;
        public Integer damageTakenPostReduction;
        public Float healing;

        public Integer kills;
        public Integer deaths;
        public Integer assists;

        public Integer lastHits;
        public Integer denies;

        public Float teamfightParticipation;

        public Integer obsPlaced;
        public Integer senPlaced;

        public Float x;
        public Float y;
        public String unit;    // hero entity name

        public List<Item> inventory = new ArrayList<>();
        // public List<Ability> abilities = new ArrayList<>();
        
        @Override
        public String toString() {
            return "Player{" +
            "key='" + key + '\'' +
            ", value=" + value +
            ", team=" + team +
            ", name='" + name + '\'' +
            ", steamId=" + steamId +
            ", heroId=" + heroId +
            ", heroVariant=" + heroVariant +
            ", level=" + level +
            ", xp=" + xp +
            ", networth=" + networth +
            ", totalGold=" + totalGold +
            ", currentGold=" + currentGold +
            ", lifeState=" + lifeState +
            ", respawnSeconds=" + respawnSeconds +
            ", buybackCooldown=" + buybackCooldown +
            ", heroDamage=" + heroDamage +
            ", towerDamage=" + towerDamage +
            ", damageTakenPreReduction=" + damageTakenPreReduction +
            ", damageTakenPostReduction=" + damageTakenPostReduction +
            ", healing=" + healing +
            ", kills=" + kills +
            ", deaths=" + deaths +
            ", assists=" + assists +
            ", lastHits=" + lastHits +
            ", denies=" + denies +
            ", teamfightParticipation=" + teamfightParticipation +
            ", obsPlaced=" + obsPlaced +
            ", senPlaced=" + senPlaced +
            ", x=" + x +
            ", y=" + y +
            ", unit='" + unit + '\'' +
            '}';
        }
    }

    public static class Item {
        public String id;
        public Integer slot;
        public Integer num_charges;
        public Integer num_secondary_charges;
    }    

    public static class Ability {
        public String id;
        public Integer abilityLevel;
    }
    
    private class UnknownItemFoundException extends RuntimeException {
        public UnknownItemFoundException(String message) {
            super(message);
        }
    }

    private class UnknownAbilityFoundException extends RuntimeException {
        public UnknownAbilityFoundException(String message) {
            super(message);
        }
    }

    private static final int TICK_RATE = 30;
    private int serverTick = -1;
    private int startTime = -1;
    private Integer time = 0;
    private int matchTime = -1;
    private boolean init = false;
    private int numPlayers = 10;
    private int[] validIndices = new int[numPlayers];
    private HashMap<Integer, Integer> slot_to_playerslot = new HashMap<>();
    private HashMap<Long, Integer> steamid_to_playerslot = new HashMap<>();

    private List<Player> playersList = new ArrayList<>();
    private Team radiantTeam = new Team(2);
    private Team direTeam = new Team(3);

    private String replayPath;
    private String outputPath;
    private int processInterval;

    private ObjectMapper mapper;
    private JsonGenerator jsonGen;

    public ReplayParser(String replayPath, int processInterval, String outputPath) {
        if (processInterval < 1) {
            throw new IllegalArgumentException("processInterval must be greater than 0");
        }
        this.replayPath = replayPath;
        this.processInterval = processInterval;
        this.outputPath = outputPath;
    }    
    
    public void parse() throws IOException {
        System.out.println("Starting to parse replay: " + replayPath);
        MappedFileSource source = new MappedFileSource(replayPath);
        long tStart = System.currentTimeMillis();
    
        mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        jsonGen = mapper.getFactory().createGenerator(new File(outputPath), JsonEncoding.UTF8);
        jsonGen.writeStartObject();
    
        try {
            SimpleRunner runner = new SimpleRunner(source);
            runner.runWith(this);
        } finally {
            jsonGen.writeEndObject();
            jsonGen.close();
            source.close();
        }
    
        System.out.println("Finished parsing replay.");
        long tEnd = System.currentTimeMillis();
        System.out.println("Time taken: " + (tEnd - tStart) + " ms");
    }
    
    @UsesStringTable("EntityNames")
    @UsesEntities
    @OnTickStart
    public void onTickStart(Context context, boolean synthetic) {
        serverTick = context.getTick();
    
        Entity gamerulesProxy = context.getProcessor(Entities.class).getByDtName("CDOTAGamerulesProxy");
        if (gamerulesProxy == null) return;

        Float gameStartTime = getEntityProperty(gamerulesProxy, "m_pGameRules.m_flGameStartTime", null);
        if (startTime == -1 && gameStartTime != null && gameStartTime > 0) {
            startTime = (int) Math.floor(gameStartTime);
        }
        if (startTime == -1) return;
    
        Integer gameState = getEntityProperty(gamerulesProxy, "m_pGameRules.m_nGameState", null);
        if (gameState == null || gameState >= 6) return;

        Float currentGameTime = getEntityProperty(gamerulesProxy, "m_pGameRules.m_fGameTime", null);

        if (currentGameTime != null) {
            time = (int) Math.floor(currentGameTime);
        } else {
            Boolean isPaused = getEntityProperty(gamerulesProxy, "m_pGameRules.m_bGamePaused", null);
            Integer pauseStartTick = getEntityProperty(gamerulesProxy, "m_pGameRules.m_nPauseStartTick", null);
            Integer totalPausedTicks = getEntityProperty(gamerulesProxy, "m_pGameRules.m_nTotalPausedTicks", null);
            if (totalPausedTicks == null) totalPausedTicks = 0;
            int effectiveTick = (isPaused != null && isPaused && pauseStartTick != null) ? pauseStartTick : serverTick;
            time = (int) Math.floor((float)(effectiveTick - totalPausedTicks) / TICK_RATE);
        }
        matchTime = time - startTime;

        if (matchTime <= 0 || (serverTick % (TICK_RATE * processInterval)) != 0) return;
    
        radiantTeam.totalCampsStacked = 0;
        radiantTeam.totalRunePickups = 0;
        radiantTeam.totalTowersKilled = 0;
        radiantTeam.totalRoshansKilled = 0;
        radiantTeam.totalSmokesUsed = 0;

        direTeam.totalCampsStacked = 0;
        direTeam.totalRunePickups = 0;
        direTeam.totalTowersKilled = 0;
        direTeam.totalRoshansKilled = 0;
        direTeam.totalSmokesUsed = 0;

        Entity playerResource = context.getProcessor(Entities.class).getByDtName("CDOTA_PlayerResource");
        if (playerResource == null) return;
    
        if (!init) {
            initializePlayers(playerResource);
        }
    
        Entity dataRadiant = context.getProcessor(Entities.class).getByDtName("CDOTA_DataRadiant");
        Entity dataDire = context.getProcessor(Entities.class).getByDtName("CDOTA_DataDire");
        
        if (!init) {
            return;
        }

        for (int i = 0; i < numPlayers; i++) {
            int idx = validIndices[i];
            Player p = playersList.get(i);
            p.team = getEntityProperty(playerResource, "m_vecPlayerData.%i.m_iPlayerTeam", idx);
            int teamSlot = getEntityProperty(playerResource, "m_vecPlayerTeamData.%i.m_iTeamSlot", idx);
            p.value = (p.team == 2 ? 0 : 128) + teamSlot;
            
            p.heroId = getEntityProperty(playerResource, "m_vecPlayerTeamData.%i.m_nSelectedHeroID", idx);
            p.heroVariant = getEntityProperty(playerResource, "m_vecPlayerTeamData.%i.m_nSelectedHeroVariant", idx);
            int handle = getEntityProperty(playerResource, "m_vecPlayerTeamData.%i.m_hSelectedHero", idx);
            
            p.level = getEntityProperty(playerResource, "m_vecPlayerTeamData.%i.m_iLevel", idx);
            p.kills = getEntityProperty(playerResource, "m_vecPlayerTeamData.%i.m_iKills", idx);
            p.deaths = getEntityProperty(playerResource, "m_vecPlayerTeamData.%i.m_iDeaths", idx);
            p.assists = getEntityProperty(playerResource, "m_vecPlayerTeamData.%i.m_iAssists", idx);
            p.teamfightParticipation = getEntityProperty(playerResource, "m_vecPlayerTeamData.%i.m_flTeamFightParticipation", idx);
            p.respawnSeconds = getEntityProperty(playerResource, "m_vecPlayerTeamData.%i.m_iRespawnSeconds", idx);

            Entity dataTeam = (p.team == 2) ? dataRadiant : dataDire;

            int currentGold = 0;
            currentGold += (int) getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iReliableGold", teamSlot);
            currentGold += (int) getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iUnreliableGold", teamSlot);
            p.currentGold = currentGold;

            p.heroDamage = getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iHeroDamage", teamSlot);
            p.towerDamage = getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iTowerDamage", teamSlot);
            p.healing = getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_fHealing", teamSlot);

            int dmgPre = 0;
            dmgPre += (int) getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iDamageByTypeReceivedPreReduction.0000", teamSlot);
            dmgPre += (int) getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iDamageByTypeReceivedPreReduction.0001", teamSlot);
            dmgPre += (int) getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iDamageByTypeReceivedPreReduction.0002", teamSlot);
            p.damageTakenPreReduction = dmgPre;

            int dmgPost = 0;
            dmgPost += (int) getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iDamageByTypeReceivedPostReduction.0000", teamSlot);
            dmgPost += (int) getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iDamageByTypeReceivedPostReduction.0001", teamSlot);
            dmgPost += (int) getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iDamageByTypeReceivedPostReduction.0002", teamSlot);
            p.damageTakenPostReduction = dmgPost;

            Float buybackExpiresAt = getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_flBuybackCooldownTime", teamSlot);
            if (buybackExpiresAt != null && time != null) {
                float cooldownRemaining = Math.max(0f, buybackExpiresAt - time);
                p.buybackCooldown = (float) Math.floor(cooldownRemaining);
            }

            p.denies = getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iDenyCount", teamSlot);
            p.obsPlaced = getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iObserverWardsPlaced", teamSlot);
            p.senPlaced = getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iSentryWardsPlaced", teamSlot);
            p.networth = getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iNetWorth", teamSlot);

            if (teamSlot >= 0) {
                p.totalGold = getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iTotalEarnedGold", teamSlot);
                p.lastHits = getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iLastHitCount", teamSlot);
                p.xp = getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iTotalEarnedXP", teamSlot);
            }
            
            Integer smokesUsed = getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iSmokesUsed", teamSlot);
            Integer campsStacked = getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iCampsStacked", teamSlot);
            Integer runePickups = getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iRunePickups", teamSlot);
            Integer towersKilled = getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iTowerKills", teamSlot);
            Integer roshansKilled = getEntityProperty(dataTeam, "m_vecDataTeam.%i.m_iRoshanKills", teamSlot);

            if (p.team == 2) {
                radiantTeam.totalCampsStacked += (campsStacked != null) ? campsStacked : 0;
                radiantTeam.totalRunePickups += (runePickups != null) ? runePickups : 0;
                radiantTeam.totalTowersKilled += (towersKilled != null) ? towersKilled : 0;
                radiantTeam.totalRoshansKilled += (roshansKilled != null) ? roshansKilled : 0;
                radiantTeam.totalSmokesUsed += (smokesUsed != null) ? smokesUsed : 0;
            } else if (p.team == 3) {
                direTeam.totalCampsStacked += (campsStacked != null) ? campsStacked : 0;
                direTeam.totalRunePickups += (runePickups != null) ? runePickups : 0;
                direTeam.totalTowersKilled += (towersKilled != null) ? towersKilled : 0;
                direTeam.totalRoshansKilled += (roshansKilled != null) ? roshansKilled : 0;
                direTeam.totalSmokesUsed += (smokesUsed != null) ? smokesUsed : 0;
            }            

            Entity heroEntity = context.getProcessor(Entities.class).getByHandle(handle);
            if (heroEntity != null) {
                Integer cx = getEntityProperty(heroEntity, "CBodyComponent.m_cellX", null);
                Integer cy = getEntityProperty(heroEntity, "CBodyComponent.m_cellY", null);
                Float vx = getEntityProperty(heroEntity, "CBodyComponent.m_vecX", null);
                Float vy = getEntityProperty(heroEntity, "CBodyComponent.m_vecY", null);
                if (cx != null && cy != null) {
                    p.x = getPreciseLocation(cx, vx);
                    p.y = getPreciseLocation(cy, vy);
                }
                p.unit = heroEntity.getDtClass().getDtName();
                p.lifeState = getEntityProperty(heroEntity, "m_lifeState", null);

                // List<Ability> abilities = getHeroAbilities(context, heroEntity);
                // p.abilities = abilities;
            
                List<Item> inventory = getHeroInventory(context, heroEntity);
                p.inventory = inventory;
            }
        }

        Entities entities = context.getProcessor(Entities.class);
        StringTable stEntityNames = context.getProcessor(StringTables.class).forName("EntityNames");

        Iterator<Entity> it = entities.getAllByPredicate(e -> {
            String dt = e.getDtClass().getDtName();
            return dt.contains("Tower") || dt.contains("Barracks") || dt.contains("Fort");
        });

        Set<String> currentBuildings = new HashSet<>();

        while (it.hasNext()) {
            Entity ent = it.next();

            Integer hp = ent.getProperty("m_iHealth");
            Integer team = ent.getProperty("m_iTeamNum");
            Integer nameIdx = ent.getProperty("m_pEntity.m_nameStringableIndex");

            if (hp == null || team == null || nameIdx == null) continue;
            if (team != 2 && team != 3) continue;

            String name = stEntityNames.getNameByIndex(nameIdx);
            if (name == null) continue;

            currentBuildings.add(name);

            Building b = new Building(name, team, hp);

            Team targetTeam = (team == 2) ? radiantTeam : direTeam;

            if (name.contains("tower1_top")) targetTeam.setBuilding("topTier1", b);
            else if (name.contains("tower2_top")) targetTeam.setBuilding("topTier2", b);
            else if (name.contains("tower3_top")) targetTeam.setBuilding("topTier3", b);

            else if (name.contains("tower1_mid")) targetTeam.setBuilding("midTier1", b);
            else if (name.contains("tower2_mid")) targetTeam.setBuilding("midTier2", b);
            else if (name.contains("tower3_mid")) targetTeam.setBuilding("midTier3", b);

            else if (name.contains("tower1_bot")) targetTeam.setBuilding("botTier1", b);
            else if (name.contains("tower2_bot")) targetTeam.setBuilding("botTier2", b);
            else if (name.contains("tower3_bot")) targetTeam.setBuilding("botTier3", b);

            else if (name.contains("tower4")) {
                if (name.contains("top")) targetTeam.setBuilding("tier4a", b);
                else if (name.contains("bot")) targetTeam.setBuilding("tier4b", b);
                else {
                    if (!targetTeam.buildings.containsKey("tier4a")) targetTeam.setBuilding("tier4a", b);
                    else targetTeam.setBuilding("tier4b", b);
                }
            }

            else if (name.contains("rax_melee_top")) targetTeam.setBuilding("topRaxMelee", b);
            else if (name.contains("rax_range_top")) targetTeam.setBuilding("topRaxRanged", b);

            else if (name.contains("rax_melee_mid")) targetTeam.setBuilding("midRaxMelee", b);
            else if (name.contains("rax_range_mid")) targetTeam.setBuilding("midRaxRanged", b);

            else if (name.contains("rax_melee_bot")) targetTeam.setBuilding("botRaxMelee", b);
            else if (name.contains("rax_range_bot")) targetTeam.setBuilding("botRaxRanged", b);

            else if (name.contains("fort")) targetTeam.setBuilding("ancient", b);
        }

        for (Map.Entry<String, Building> entry : radiantTeam.buildings.entrySet()) {
            if (!currentBuildings.contains(entry.getValue().name)) {
                radiantTeam.setBuilding(entry.getKey(), new Building(entry.getValue().name, 2, 0));
            }
        }

        for (Map.Entry<String, Building> entry : direTeam.buildings.entrySet()) {
            if (!currentBuildings.contains(entry.getValue().name)) {
                direTeam.setBuilding(entry.getKey(), new Building(entry.getValue().name, 3, 0));
            }
        }

        radiantTeam.observerWards.clear();
        direTeam.observerWards.clear();

        Iterator<Entity> wards = entities.getAllByDtName("CDOTA_NPC_Observer_Ward");
        while (wards.hasNext()) {
            Entity ward = wards.next();
            Integer team = ward.getProperty("m_iTeamNum");
            Integer lifeState = ward.getProperty("m_lifeState");

            if (team != null && lifeState != null && lifeState == 0) {
                Float x = getPreciseLocation(
                    ward.getProperty("CBodyComponent.m_cellX"),
                    ward.getProperty("CBodyComponent.m_vecX")
                );
                Float y = getPreciseLocation(
                    ward.getProperty("CBodyComponent.m_cellY"),
                    ward.getProperty("CBodyComponent.m_vecY")
                );
                Map<String, Float> pos = new HashMap<>();
                pos.put("x", x);
                pos.put("y", y);

                if (team == 2) {
                    radiantTeam.observerWards.add(pos);
                } else if (team == 3) {
                    direTeam.observerWards.add(pos);
                }
            }
        }

        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("radiant", deepCopyTeam(radiantTeam));
        snapshot.put("dire", deepCopyTeam(direTeam));
        try {
            jsonGen.writeFieldName(String.valueOf(matchTime));
            mapper.writeValue(jsonGen, snapshot);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write JSON incrementally at matchTime=" + matchTime, e);
        }
    }
    
    private void initializePlayers(Entity playerResource) {
        int added = 0;
        int i = 0;
        // Safety check in case of unexpected player counts.
        while (added < numPlayers && i < 30) {
            try {
                int playerTeam = getEntityProperty(playerResource, "m_vecPlayerData.%i.m_iPlayerTeam", i);
                int teamSlot = getEntityProperty(playerResource, "m_vecPlayerTeamData.%i.m_iTeamSlot", i);
                Long steamId = getEntityProperty(playerResource, "m_vecPlayerData.%i.m_iPlayerSteamID", i);
                String playerName = getEntityProperty(playerResource, "m_vecPlayerData.%i.m_iszPlayerName", i);

                if (playerTeam == 2 || playerTeam == 3) {
                    Player player = new Player();
                    player.key = String.valueOf(added);
                    player.value = (playerTeam == 2 ? 0 : 128) + teamSlot;
                    player.team = playerTeam;
                    player.name = playerName;
                    player.steamId = steamId;
                    validIndices[added] = i;
                    slot_to_playerslot.put(added, player.value);
                    steamid_to_playerslot.put(steamId, player.value);
                    playersList.add(player);
                    added++;

                    if (playerTeam == 2) {
                        radiantTeam.players.add(player);
                    } else if (playerTeam == 3) {
                        direTeam.players.add(player);
                    }                    
                }
            } catch (Exception e) {
                System.out.println("Error while initializing player " + i + ": " + e.getMessage());
            }
            i++;
        }
        if (added < numPlayers) {
            throw new IllegalStateException("Not enough players found in the replay. Expected " + numPlayers + ", but found " + added);
        }
        init = true;
    }

    public <T> T getEntityProperty(Entity e, String property, Integer idx) {
        try {
            if (e == null) {
                return null;
            }
            if (idx != null) {
                property = property.replace("%i", Util.arrayIdxToString(idx));
            }
            FieldPath fp = e.getDtClass().getFieldPathForName(property);
            if (fp == null) {
                return null;
            }
            return e.getPropertyForFieldPath(fp);
        } catch (Exception ex) {
            return null;
        }
    }

    private List<Item> getHeroInventory(Context ctx, Entity eHero) {
        List<Item> inventoryList = new ArrayList<>(6);

        for (int i = 0; i < 8; i++) {
            try {
                Item item = getHeroItem(ctx, eHero, i);
                if (item != null) {
                    inventoryList.add(item);
                }
            } catch (Exception e) {
                System.err.println(e);
            }
        }

        return inventoryList;
    }

    private List<Ability> getHeroAbilities(Context ctx, Entity eHero) {
        List<Ability> abilityList = new ArrayList<>(32);
        
        Set<String> filteredAbilityPrefixes = Set.of(
            "ability_", "plus_", "generic_", "twin_gate_", "abyssal_underlord_portal", "special_bonus_", "attribute_bonus"
        );
        
        for (int i = 0; i < 32; i++) {
            try {
                Ability ability = getHeroAbilities(ctx, eHero, i);
                if (ability != null && ability.id != null) {
                    boolean isFiltered = filteredAbilityPrefixes.stream().anyMatch(prefix -> ability.id.startsWith(prefix));
                    if (!isFiltered) {
                        abilityList.add(ability);
                    }
                }
            } catch (Exception e) {
                System.err.println(e);
            }
        }
        return abilityList;
    }    

    private Ability getHeroAbilities(Context ctx, Entity eHero, int idx) throws UnknownAbilityFoundException {
        StringTable stEntityNames = ctx.getProcessor(StringTables.class).forName("EntityNames");
        Entities entities = ctx.getProcessor(Entities.class);

        Integer hAbility;
        if (eHero.hasProperty("m_hAbilities." + Util.arrayIdxToString(idx))) {
            hAbility = eHero.getProperty("m_hAbilities." + Util.arrayIdxToString(idx));
        } else if (eHero.hasProperty("m_vecAbilities." + Util.arrayIdxToString(idx))) {
            hAbility = eHero.getProperty("m_vecAbilities." + Util.arrayIdxToString(idx));
        } else {
            hAbility = 0xFFFFFF;
        }
        
        if (hAbility == 0xFFFFFF) {
            return null;
        }
        
        Entity eAbility = entities.getByHandle(hAbility);
        if (eAbility == null) {
            throw new UnknownAbilityFoundException(String.format("Can't find ability by its handle (%d)", hAbility));
        }
        String abilityName = stEntityNames.getNameByIndex(eAbility.getProperty("m_pEntity.m_nameStringableIndex"));
        if (abilityName == null) {
            throw new UnknownAbilityFoundException("Can't get ability name from EntityName string table");
        }

        Ability ability = new Ability();
        ability.id = abilityName;
        ability.abilityLevel = eAbility.getProperty("m_iLevel");

        return ability;
    }

    private Item getHeroItem(Context ctx, Entity eHero, int idx) throws UnknownItemFoundException {
        StringTable stEntityNames = ctx.getProcessor(StringTables.class).forName("EntityNames");
        Entities entities = ctx.getProcessor(Entities.class);

        Integer hItem = eHero.getProperty("m_hItems." + Util.arrayIdxToString(idx));
        if (hItem == 0xFFFFFF) {
            return null;
        }
        Entity eItem = entities.getByHandle(hItem);
        if (eItem == null) {
            throw new UnknownItemFoundException(String.format("Can't find item by its handle (%d)", hItem));
        }
        String itemName = stEntityNames.getNameByIndex(eItem.getProperty("m_pEntity.m_nameStringableIndex"));
        if (itemName == null) {
            throw new UnknownItemFoundException("Can't get item name from EntityName string table");
        }

        Item item = new Item();
        item.id = itemName;
        item.slot = idx;
        int numCharges = eItem.getProperty("m_iCurrentCharges");
        if (numCharges != 0) {
            item.num_charges = numCharges;
        }
        int numSecondaryCharges = eItem.getProperty("m_iSecondaryCharges");
        if (numSecondaryCharges != 0) {
            item.num_secondary_charges = numSecondaryCharges;
        }
        return item;
    }

    private Team deepCopyTeam(Team original) {
        Team copy = new Team(original.teamId);
        for (Player p : original.players) {
            Player np = mapper.convertValue(p, Player.class);
            copy.players.add(np);
        }
        copy.buildings.putAll(original.buildings);
        copy.observerWards = new ArrayList<>(original.observerWards);
        copy.totalCampsStacked = original.totalCampsStacked;
        copy.totalRunePickups = original.totalRunePickups;
        copy.totalTowersKilled = original.totalTowersKilled;
        copy.totalRoshansKilled = original.totalRoshansKilled;
        copy.totalSmokesUsed = original.totalSmokesUsed;
        return copy;
    }

    private Float getPreciseLocation (Integer cell, Float vec) {
        return (cell*128.0f+vec)/128;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: java -jar parser.jar <replay.dem> <output.json>");
            return;
        }
        String replayPath = args[0];
        String outputPath = args[1];
        ReplayParser parser = new ReplayParser(replayPath, 30, outputPath);
        parser.parse();
    }
}
