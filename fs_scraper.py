import os
import time

import re
import logging
import yaml
from unidecode import unidecode
from tqdm import tqdm
from pathlib import Path
from typing import Dict, List, Any, Tuple

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from pymongo import MongoClient

CLIENT = MongoClient()
MY_DTB = CLIENT['soccer_analysis']
MY_COL = MY_DTB['matchs']

FS_URL = 'https://www.flashscore.fr'
CUR_OUT_PATH = '\\'.join(os.path.realpath(__file__).split('\\')[:-1]) + '\\output'

if not os.path.exists(CUR_OUT_PATH):
    os.mkdir(CUR_OUT_PATH)

logging.basicConfig(
    level=logging.INFO,
    filename="scraper.log",
    filemode="w",
    format='%(asctime)s - %(levelname)s - %(message)s'
    )

class FlashScoreScraper:
    def __init__(
        self,
        url_res_league: str,
        options: Options,
        service: Service,
        export_to_yaml: bool,
        export_to_dtb: bool,
        ) -> None:

        self.driver = webdriver.Chrome(
            options=options, 
            service=service
            )
        self.url_res_league = url_res_league
        self.driver.get(url_res_league)
        self.season = (re.search('\d{4}-\d{4}', url_res_league)[0] if re.search('\d{4}-\d{4}', url_res_league) else 'Not_found')
        self.export_yaml = export_to_yaml
        self.export_dtb = export_to_dtb
        # self.extend_whole_page()

    def extend_whole_page(self):
        xpath = "//a[@class='event__more event__more--static']"

        while self.driver.find_elements(By.XPATH, xpath):
            try: 
                element = WebDriverWait(self.driver, 10).until(expected_conditions.element_to_be_clickable((
                    By.XPATH, 
                    xpath
                    )))
                element.click()
            except:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    def parse_matchs(self) -> Dict:
        match_no_last_xpath = "//div[@class='event__match event__match--static event__match--twoLine']"
        match_last_xpath = "//div[@class='event__match event__match--static event__match--last event__match--twoLine']"
        match_no_last_list = self.driver.find_elements(By.XPATH, match_no_last_xpath)
        match_last_list = self.driver.find_elements(By.XPATH, match_last_xpath)
        match_list = match_no_last_list + match_last_list

        id_list = [match.get_attribute('id')[-8:] for match in match_list]
        url_match_resume_page_list = list(map(
            lambda x: FS_URL + '/match/' + x + '/#/resume-du-match/resume-du-match', 
            id_list
            ))
        url_match_stat_match_page_list = list(map(
            lambda x: FS_URL + '/match/' + x + '/#/resume-du-match/statistiques-du-match/0', 
            id_list
            ))
        url_match_compo_page_list = list(map(
            lambda x: FS_URL + '/match/' + x + '/#/resume-du-match/compositions', 
            id_list
            ))
        url_odd_1x2_regtime_page_list = list(map(
            lambda x: FS_URL + '/match/' + x + '/#/comparaison-des-cotes/cotes-1x2/temps-regulier', 
            id_list
            ))
        url_onetoone_global_page_list = list(map(
            lambda x: FS_URL + '/match/' + x + '/#/tete-a-tete/overall', 
            id_list
            ))
        matchs_urls = [
            url_match_resume_page_list,
            url_match_stat_match_page_list,
            url_match_compo_page_list,
            url_odd_1x2_regtime_page_list,
            url_onetoone_global_page_list,
            ]
        matchs_urls = list(map(list, list(zip(*matchs_urls))))

        print(f'Scraping of {self.url_res_league}')
        for idx in tqdm(range(len(matchs_urls)), ncols=100, desc='Scraping in progress'):
            match_data = self.parse_match(matchs_urls[idx], id_list[idx])
            
            if self.export_yaml:
                self.export_to_yaml(match_data, id_list[idx])

            if self.export_dtb:
                self.export_to_dtb(match_data)

        self.driver.close()

    def parse_match(
        self, 
        match_urls: str,
        match_id: str,
        ) -> Dict:

        match_data = {}
        match_data['saison'] = self.season
        match_data['id'] = match_id

        self.driver.get(match_urls[0])
        time.sleep(1)
        self.scrape_infos_gen_page(match_data)

        self.driver.get(match_urls[0])
        time.sleep(1)
        self.scrape_match_resume_page(match_data)

        self.driver.get(match_urls[1])
        time.sleep(1)
        self.scrape_match_stat_match_page(match_data)

        self.driver.get(match_urls[2])
        time.sleep(1)
        self.scrape_match_compo_page(match_data)

        self.driver.get(match_urls[3])
        time.sleep(1)
        self.scrape_odds_1x2_regtime_page(match_data)

        # # Renvoie les matchs actuels
        # self.driver.get(match_urls[4])
        # time.sleep(1)
        # self.scrape_onetoone_global_page(match_data)

        return match_data

    def scrape_infos_gen_page(
        self, 
        match_data: Dict,
        ) -> None:

        country, league, round = self.scrape_context()
        match_data['country'] = country
        match_data['league'] = league
        match_data['round'] = round

        start_day, start_hour = self.scrape_start_time()
        match_data['start_day'] = start_day
        match_data['start_hour'] = start_hour

        home_team_name, away_team_name = self.scrape_team_name()
        match_data['home_team_name'] = home_team_name
        match_data['away_team_name'] = away_team_name
        
        home_team_goals, away_team_goal = self.scrape_final_score()
        match_data['home_team_goals'] = home_team_goals
        match_data['away_team_goal'] = away_team_goal

        match_status = self.scrape_match_status()
        match_data['match_status'] = match_status

        info_box = self.scrape_info_box()
        match_data['info_box'] = (info_box if info_box else '')

    def scrape_context(self) -> Tuple[str, str, str]:
        context_xpath = "//span[@class='tournamentHeader__country']"
        context_list = self.driver.find_elements(By.XPATH, context_xpath)

        try: 
            research_country = re.search('^.*(?=:.*)', context_list[0].text)
            research_league = re.search('(?<=: ).*(?= -)', context_list[0].text)
            research_round = re.search('(?<= - ).*$', context_list[0].text)
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: context_list: {context_list[0]}')
            # print(f'Error: context_list: {context_list}')

        country = (unidecode(research_country[0]).lower() if research_country else 'Not_found')
        league = (unidecode(research_league[0]).lower() if research_league else 'Not_found')
        round = (unidecode(research_round[0]).lower() if research_round else 'Not_found')

        return country, league, round

    def scrape_start_time(self) -> Tuple[str, str]:
        start_time_xpath = "//div[@class='duelParticipant__startTime']"
        start_time_list = self.driver.find_elements(By.XPATH, start_time_xpath)

        try:
            start_day, start_hour = start_time_list[0].text.split(' ')
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: start_time_list: {start_time_list[0]}')
            # print(f'Error: start_time_list: {start_time_list}')

        return start_day, start_hour

    def scrape_team_name(self) -> Tuple[str, str]:
        team_name_xpath = "//div[@class='participant__participantName participant__overflow']"
        team_name_list = self.driver.find_elements(By.XPATH, team_name_xpath)
        
        try:
            home_team_name, away_team_name = (
                unidecode(team_name_list[0].text).lower(), 
                unidecode(team_name_list[1].text).lower()
                )
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: team_name_list: {team_name_list}')
            # print(f'Error: team_name_list: {team_name_list}')

        return home_team_name, away_team_name

    def scrape_final_score(self) -> Tuple[int, int]:
        final_score_xpath = "//div[@class='detailScore__wrapper']"
        final_score_list = self.driver.find_elements(By.XPATH, final_score_xpath)

        try:
            home_team_goals, away_team_goal = list(map(
                int, 
                final_score_list[0].text.split('\n-\n')
                ))
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: final_score_list: {final_score_list}')
            # print(f'Error: final_score_list: {final_score_list}')

        return home_team_goals, away_team_goal

    def scrape_match_status(self) -> str:
        match_status_xpath = "//div[@class='detailScore__status']"
        match_status_list = self.driver.find_elements(By.XPATH, match_status_xpath)

        try:
            match_status = unidecode(match_status_list[0].text).lower()
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: match_status_list: {match_status_list}')
            # print(f'Error: match_status_list: {match_status_list}')

        return match_status

    def scrape_info_box(self) -> str:
        info_box_xpath = "//div[@class='infoBox__info']"
        info_box_list = self.driver.find_elements(By.XPATH, info_box_xpath)

        try:
            info_box = unidecode(info_box_list[0].text).lower()
            return info_box

        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: info_box_list: {info_box_list}')
            return None        

    def scrape_match_resume_page(
        self,
        match_data: Dict,
        ) -> None:
        
        goals_by_period = self.scrape_scores_by_period()
        match_data['goals_by_period'] = goals_by_period

        events = self.scrape_events()
        match_data['events'] = events

        infos_match = self.scrape_infos_match()
        match_data['referee'] = infos_match[0]
        match_data['stadium'] = infos_match[1]
        if len(infos_match) == 3:
            match_data['spectators'] = infos_match[2]

    def scrape_scores_by_period(self) -> Dict:

        match_scores_xpath = "//div[@class='smv__incidentsHeader section__title']"
        match_scores_list = self.driver.find_elements(By.XPATH, match_scores_xpath)

        try: 
            goals_by_period = {}
            for i in range(len(match_scores_list)):
                period, score = match_scores_list[i].text.split('\n')
                goals = score.split(' - ')
                goals_by_period.update({
                    unidecode(period).lower(): {
                        'home_goal': int(goals[0]),
                        'away_goal': int(goals[1]),
                        }
                    })
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: match_scores_list: {match_scores_list}')
            # print(f'Error: match_scores_list: {match_scores_list}')

        return goals_by_period

    def scrape_events(self) -> List[Dict]:

        event_xpath = "//div[@class='smv__incident']"
        event_list = self.driver.find_elements(By.XPATH, event_xpath)

        try: 
            events = []
            for raw_event in event_list:
                event_elem = raw_event.text.split('\n')
                if len(event_elem[1].split(' - ')) == 2: # But temps règlementaire
                    if len(event_elem) == 3:
                        events.append({
                            'type': 'goal',
                            'time': event_elem[0],
                            'score': event_elem[1],
                            'scorer_name': unidecode(event_elem[2]).lower(),
                            })
                    else: 
                        events.append({
                            'type': 'goal',
                            'time': event_elem[0],
                            'score': event_elem[1],
                            'scorer_name': unidecode(event_elem[2]).lower(),
                            'passer_name': re.sub('\(|\)', '', unidecode(event_elem[3]).lower()),
                            })
                elif (not event_elem[0].endswith("\'")) and (unidecode(event_elem[-1]).lower().endswith('manque)')): # Pénalty manqué TAB
                    events.append({
                        'type': 'penalty manque tab',
                        'time': event_elem[0],
                        'striker_name': unidecode(event_elem[1]).lower(),
                        })
                elif (not event_elem[0].endswith("\'")) and (unidecode(event_elem[-1]).lower().endswith('(penalty)')): # Pénalty marqué TAB
                    events.append({
                        'type': 'penalty marque tab',
                        'time': event_elem[0],
                        'striker_name': unidecode(event_elem[1]).lower(),
                        })
                elif (len(event_elem) == 3) and (not event_elem[2].startswith("(")): # Substitute
                    events.append({
                        'type': 'substitute',
                        'time': event_elem[0],
                        'sub_in': unidecode(event_elem[1]).lower(),
                        'sub_out': unidecode(event_elem[2]).lower(),
                        })
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: event_list: {event_list}')
            # print(f'Error: event_list: {event_list}')

        return events

    def scrape_infos_match(self) -> Tuple[str, str, str]:

        infos_match_xpath = "//div[@class='mi__data']"
        infos_match_list = self.driver.find_elements(By.XPATH, infos_match_xpath)

        try:
            infos_match = infos_match_list[0].text.split('\n')
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: infos_match_list: {infos_match_list}')
            # print(f'Error: infos_match_list: {infos_match_list}')

        try:
            referee = unidecode(infos_match[1]).lower()
            stadium = unidecode(infos_match[3]).lower()
            spectators = int(infos_match[5].replace(' ', ''))
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: infos_match: {infos_match}')
            # print(f'Error: infos_match: {infos_match}')
            return referee, stadium
        else:
            return referee, stadium, spectators

    def scrape_match_stat_match_page(
        self,
        match_data: Dict,
        ) -> None:

        stats_xpath = "//div[@class='section']"
        stats_list = self.driver.find_elements(By.XPATH, stats_xpath)

        try:
            stats = stats_list[0].text.split('\n')
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: stats_list: {stats_list}')
            # print(f'Error: stats_list: {stats_list}')

        match_data['stats'] = {
            unidecode(stats[3*i+1]).lower(): {'home_team': float(stats[3*i]), 'away_team': float(stats[3*i+2])} 
            if stats[3*i].isdigit()
            else {'home_team': float(stats[3*i][:-1]), 'away_team': float(stats[3*i+2][:-1])} 
            for i in range(len(stats)//3)
            }
        
    def scrape_match_compo_page(
        self,
        match_data: Dict,
        ) -> None:

        formation_xpath = "//div[@class='lf__header section__title']"
        formation_list = self.driver.find_elements(By.XPATH, formation_xpath)
        try: 
            formations = formation_list[0].text.split('\n')
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: formation_list: {formation_list}')
            # print(f'Error: formation_list: {formation_list}')
        
        match_data['home_formation'] = formations[0]
        match_data['away_formation'] = formations[2]

        compo_xpath = "//div[@class='lf__fieldWrap']"
        compo_list = self.driver.find_elements(By.XPATH, compo_xpath)
        try: 
            compos = compo_list[0].text.split('\n')
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: compo_list: {compo_list}')
            # print(f'Error: compo_list: {compo_list}')

        home_compo = compos[:len(compos)//2]
        away_compo = compos[len(compos)//2:]
        match_data['home_holders'] = [
            {'name': unidecode(home_compo[2*i+1]).lower(), 'num': home_compo[2*i]}
            for i in range(len(home_compo)//2)
            ]
        match_data['away_holders'] = [
            {'name': unidecode(away_compo[2*i+1]).lower(), 'num': away_compo[2*i]}
            for i in range(len(away_compo)//2)
            ]

        other_infos_xpath = "//div[@class='lf__side']"
        other_infos_list = self.driver.find_elements(By.XPATH, other_infos_xpath)
        try: 
            home_subs = list(filter(lambda x: not x.startswith('('), other_infos_list[2].text.split('\n')))
            away_subs = list(filter(lambda x: not x.startswith('('), other_infos_list[3].text.split('\n')))
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: other_infos_list: {other_infos_list}')
            # print(f'Error: other_infos_list: {other_infos_list}')

        match_data['home_subs'] = [
            {'name': unidecode(home_subs[2*i+1]).lower(), 'num': home_subs[2*i]}
            for i in range(len(home_subs)//2)
            ]
        match_data['away_subs'] = [
            {'name': unidecode(away_subs[2*i+1]).lower(), 'num': away_subs[2*i]}
            for i in range(len(away_subs)//2)
            ]

        try: 
            home_absent_list = list(filter(lambda x: not x.startswith('('), other_infos_list[4].text.split('\n')))
            away_absent_list = list(filter(lambda x: not x.startswith('('), other_infos_list[5].text.split('\n')))
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: other_infos_list: {other_infos_list}')
            # print(f'Error: other_infos_list: {other_infos_list}')

        if len(home_absent_list) > 0:
            match_data['home_absents'] = [unidecode(home_absent_list[i]).lower() for i in range(len(home_absent_list))]
        if len(away_absent_list) > 0:
            match_data['away_absents'] = [unidecode(away_absent_list[i]).lower() for i in range(len(away_absent_list))]

        try: 
            home_coach = other_infos_list[6].text
            away_coach = other_infos_list[7].text
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: other_infos_list: {other_infos_list}')
            # print(f'Error: other_infos_list: {other_infos_list}')

        match_data['home_coach'] = unidecode(home_coach).lower()
        match_data['away_coach'] = unidecode(away_coach).lower()

    def scrape_odds_1x2_regtime_page(
        self, 
        match_data: Dict,
        ) -> None:
        
        odds_xpath = "//div[@class='ui-table__row']"
        bookmaker_xpath = "//a[@class='prematchLink']"
        odd_choice_xpath = "//div[@class='ui-table__header']"
        odds_list = self.driver.find_elements(By.XPATH, odds_xpath)
        bookmaker_list = self.driver.find_elements(By.XPATH, bookmaker_xpath)
        odd_choice_list = self.driver.find_elements(By.XPATH, odd_choice_xpath)
        odds = []
        bookmakers = []

        try:
            odd_choice = odd_choice_list[0].text.split('\n')[1:]
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: odds_list: {odds_list}, bookmaker_list: {bookmaker_list}, odd_choice_list: {odd_choice_list}')
            # print(f'Error: odds_list: {odds_list}, bookmaker_list: {bookmaker_list}, odd_choice_list: {odd_choice_list}')

        for odd, bookmaker in zip(odds_list, bookmaker_list):
            bookmakers.append(bookmaker.get_attribute('title'))
            odds.append(dict(zip(odd_choice, map(float, odd.text.split('\n')))))

        if 'odds' not in match_data:
            match_data['odds'] = {}
        match_data['odds']['1x2'] = {'regular_time': dict(zip(bookmakers, odds))}
        

    def scrape_onetoone_global_page(
        self,
        match_data: Dict,
        ) -> None:

        onetoone_global_xpath = "//div[@class='h2h__section section ']"
        onetoone_global_list = self.driver.find_elements(By.XPATH, onetoone_global_xpath)

        try:
            home_team_last_match_list = onetoone_global_list[0].text.split('\n')[1:-1]
            away_team_last_match_list = onetoone_global_list[1].text.split('\n')[1:-1]
            last_duel_list = onetoone_global_list[2].text.split('\n')[1:-1]
        except IndexError as e:
            logging.info(e)
            logging.info(f'Error: onetoone_global_list: {onetoone_global_list}')
            # print(f'Error: onetoone_global_list: {onetoone_global_list}')

        if 'one_to_one' not in match_data:
            match_data['one_to_one'] = {}
        if 'global' not in match_data['one_to_one']:
            match_data['one_to_one']['global'] = {}
                
        match_data['one_to_one']['global']['home_team_last_matchs'] = [
            {
                'date': home_team_last_match_list[7*i],
                'context': unidecode(home_team_last_match_list[7*i+1]).lower(),
                'home_team_name': unidecode(home_team_last_match_list[7*i+2]).lower(),
                'away_team_name': unidecode(home_team_last_match_list[7*i+3]).lower(),
                'home_goals': unidecode(home_team_last_match_list[7*i+4]).lower(),
                'away_goals': unidecode(home_team_last_match_list[7*i+5]).lower(),
                'result': unidecode(home_team_last_match_list[7*i+6]).lower(),
                }
            for i in range(len(home_team_last_match_list)//7)
            ]

        match_data['one_to_one']['global']['away_team_last_matchs'] = [
            {
                'date': away_team_last_match_list[7*i],
                'context': unidecode(away_team_last_match_list[7*i+1]).lower(),
                'home_team_name': unidecode(away_team_last_match_list[7*i+2]).lower(),
                'away_team_name': unidecode(away_team_last_match_list[7*i+3]).lower(),
                'home_goals': unidecode(away_team_last_match_list[7*i+4]).lower(),
                'away_goals': unidecode(away_team_last_match_list[7*i+5]).lower(),
                'result': unidecode(away_team_last_match_list[7*i+6]).lower(),
                }
            for i in range(len(away_team_last_match_list)//7)
            ]

        match_data['one_to_one']['global']['last_duel'] = [
            {
                'date': last_duel_list[6*i],
                'context': unidecode(last_duel_list[6*i+1]).lower(),
                'home_team_name': unidecode(last_duel_list[6*i+2]).lower(),
                'away_team_name': unidecode(last_duel_list[6*i+3]).lower(),
                'home_goals': unidecode(last_duel_list[6*i+4]).lower(),
                'away_goals': unidecode(last_duel_list[6*i+5]).lower(),
                }
            for i in range(len(last_duel_list)//6)
            ]

    def export_to_yaml(
        self, 
        data: Dict,
        id: str,
        ) -> None:

        with open(CUR_OUT_PATH + f'\\{id}.yaml', 'w') as f:
            yaml.dump(data, f, sort_keys=False)

    def export_to_dtb(
        self,
        data: Dict,
        ) -> None:
        
        MY_COL.insert_one(data)


if __name__ == '__main__':

    years = [(2017-i, 2018-i) for i in range(1)]
    url_leagues = ['https://www.flashscore.fr/football/france/ligue-1-'+ str(year[0]) + '-' + str(year[1]) + '/resultats/' for year in years]
    opts = Options()
    service = Service(executable_path=ChromeDriverManager().install())
    export_to_yaml = False
    export_to_dtb = True

    for url_league in url_leagues:
        scraper = FlashScoreScraper(
            url_league,
            opts, 
            service,
            export_to_yaml,
            export_to_dtb,
            )
        time.sleep(20)
        scraper.parse_matchs()