"""Conservative tournament mentions with article evidence, computed locally.

Reviewed aliases represent a series (all editions). Unrecognized names retain
their qualifiers and edition wording; no fuzzy merging or inferred event dates.
"""
import hashlib
import re
import unicodedata

VERSION = 'tournaments-v1'
ALIASES = {
    'World Amateur Go Championship': ('World Amateur Go Championship', 'World Amateur Championship', 'WAGC'),
    "Korean Prime Minister's Cup": ("Korean Prime Minister's Cup", "Korea Prime Minister's Cup",
                                     "Korean Prime Minister’s Cup", "Korean Prime Minister's Championship", 'KPMC'),
    'World Collegiate Weiqi Championship': ('World Collegiate Weiqi Championship', 'World University Go Championship',
        'World University Go-Weiqi Championship', 'World University Championship in Go-Weiqi'),
    'Pandanet Go European Team Championship': ('Pandanet Go European Team Championship',
        'Pandanet European Team Championship', 'PGETC'),
    'European Go Congress': ('European Go Congress', 'EGC'),
    'US Go Congress': ('US Go Congress', 'U.S. Go Congress'),
    'European Professional Go Championship': ('European Professional Go Championship', 'European Pro Championship',
        'European Championship among Go Professionals', 'European Championship among professionals in Go'),
    'Nongshim Shin Ramyun Cup': ('Nongshim Shin Ramyun Cup', 'Nongshim Cup'),
}
# Names observed in the indexed corpus and reviewed for the initial directory.
# Each row is canonical name | explicitly supported spelling variants. Edition
# numbers are not guessed; a reviewed series deliberately includes all editions.
_REVIEWED = """
European Go Championship|European Championship
Pandanet Go European Team Championship|European Team Championship
Korean Prime Minister's Cup|Korea Prime Minister Cup|Korean Prime Minister Cup|Korea Prime Minister’s Cup|Korean Prime-Minister Cup
Canadian Open|Canadian Go Open
US Open|U.S. Open
Chicago Open
Maryland Open
San Diego Go Championship
San Diego Open
San Jose Open
SoCal Open
Rhode Island State Championship
Providence Spring Tournament
Vermont Summer Go Tournament
Vermont Spring Go Tournament
Vermont State Championship
North American Professional Qualification Tournament|North American Go Federation Pro Qualification Tournament|North American Pro Qualification Tournament|NAGF Pro Qualification Tournament|North American Go Federation Professional Qualification Tournament
North American Professional Championship|North American Pro Championship
North American Online League|NAOL
Pandanet-AGA City League|AGA City League
Redmond Cup
Girls Cup
California State Championship
South Carolina Go Championship
Greater Boston Youth Go Tournament
Seattle Go Center Spring Tournament
Texas Championship
INAF Cup
Samsung Cup
NHK Cup
Agon Kiriyama Cup
Golden Mean Cup
Yangming Cup
Silk Road International City Weiqi Tournament|Silk Road International City Weiqi Open|Silk Road International Tournament|Silk Road Tournament
Nanyang Cup
MLILY Cup|MLily Cup|Mlily Cup
Globis Cup|GLOBIS Cup
LG Cup
Chunlan Cup
Ing Cup
Wu Qingyuan Cup|Go Seigen Cup
Shinhan Bank World Championship|Shinhan Bank Championship
Quzhou Lanke Cup
Lanke Cup
Four Seas Cup
Haifeng Cup
Changqi Cup
Guanyin Cup
Qingshui Guanyin Cup
Teikei Group Cup|Teikei Cup
Gigi Auction Cup|Jigi Auction Cup
KBF Baduk League
NH Nonghyup Bank Korea Women's Baduk League|NH Nonghyup Bank Korea Women's Go League
Sopalcosanol Legend League|Soparcosanol Legend League
Shanhai Cup
CITIC Cup
Matsuda Cup
Araki Cup
Pandanet Cup
Neo-Human Pair Go Tournament|Neohuman Pair Go Tournament
Senko Cup
Whampoa Cup National Weiqi Championships|Whampoa Cup National Weiqi Competition|Whampoa Cup
CP ALL CUP
Southeast Asia Weiqi Championship
Thai International Go Congress|Thailand International Go Congress
Shin-An World Baduk Masters
Paris International Go Tournament|Paris International Tournament
Pisa International Weiqi Cup|Pisa International Wei-qi Cup
Dutch Open
Welsh Open
British Go Congress
British Open
British Championship
British Lightning Championship
Cheshire Open
Cheshire Tournament
Ulster Open|IGA Ulster Open
Leinster Open
Irish Championship
IGA Online League
Embassy of Japan in Ireland Tournament
Durham Go Tournament|Durham Tournament
Prague Go Tournament|Prague Tournament
Old Hunter's Cup|Old Hunter’s Cup|Old Hunters Cup
Ghent Go Tournament
Belgian Go Championship
Lanke Vienna Open|Lanke Cup Vienna Open
Moyo Open
Kiel Go Tournament
Schleswig-Holstein State Championship
DSK Cup|DenkSportKultur Cup
Japanese Consul General Cup
Cologne Tournament
Lanke German Masters|German Lanke Masters
French Youth Championship
French Women's Championship
French Open Championship
European Youth Championship|European Youth Go Championship
European Pair Go Championship|European Pair-Go Championship|European Pair-go Championship
European Youth Team Championship
Ukrainian Go Championship
Ukrainian Women's Go Championship
Ukrainian Mixed Doubles Championship|Ukraine Mixed Doubles Championship
Ukrainian Cup
Latin American Go Congress
Ibero-American Go Championship|Ibero American Go Championship
Ibero-American Open Go Championship
Latin American Youth Go Championship|Latin American Youth Championship
Latin American Women's Go Championship
Latin American Pandanet Go Team League|Pandanet Go Latin American Team Championship|Latin American Team Go League
World Student Go Oza Championship|World Students Go OZA Championship|World Students GO OZA Championship
World Amateur Pairs Championship
International Pair Go Championship
Mexican Open Go Tournament
Santiago Open
Factorio Speed Go Championship
TIGGRE - Ellie Cup|TIGGRE – Ellie Cup
Kagaribi Cup
Beihai Xinyi Cup
Corona Cup
GS Caltex Cup
CCTV Cup
Ryusei Tournament
Kiwang Tournament
Mingren Tournament
"""
for _line in _REVIEWED.strip().splitlines():
    _name, *_variants = _line.split('|')
    ALIASES[_name] = tuple(dict.fromkeys((*ALIASES.get(_name, ()), _name, *_variants)))
TOKEN = r"[A-ZÀ-ÖØ-ÞĀ-ž][\w’'−–-]*"
NUMBER = r'\d+(?:st|nd|rd|th)?'
CONNECTOR = r'(?:of|the|in|for|and|de|del|at)'
WORD = rf'(?:{TOKEN}|{NUMBER}|{CONNECTOR})'
ENDING = r'(?:Championships?|Tournament|Cup|League|Congress|Open|Masters)'
PHRASE = re.compile(rf'\b(?:{TOKEN}|{NUMBER})(?:[ \t]+{WORD}){{0,13}}[ \t]+{ENDING}\b')
BAD = set('results result registration announcement report reports news overview schedule congratulates congratulations '
          'wins win won winner winners winning takes holds captures defends join joins team teams '
          'welcome looking forward awards award ceremony organized organising organizer sponsors sponsored '
          'sponsor chairman president secretary director rules regulations format title titles reigning '
          'former reigning defending nationalities website information applications interviews interview '
          'about upcoming next previous last this these those our their my your his her more new '
          'photo picture caption view views dates date during held hold hosts host participant participants '
          'player players promoted promotion details preliminary final finals first second third fourth '
          'fifth sixth seventh eighth ninth tenth finalised concluded champions champion at'.split())
BAD.update('claims lead leads highlights reached regained upsets following finishes watch call apply list '
           'standings pay towards summary organizing start end now organising with as to from by held '
           'following following strength newest inaugural sunny td commentary matchups supplementary '
           'key application applications officer bonus historic dramatic kill tournament hosting'.split())
GENERIC = set('the a an go weiqi wei-qi baduk national international world european american asian '
              'amateur professional youth junior senior student university collegiate women womens men mens '
              'female male team teams pair mixed open online annual tournament tournaments championship '
              'championships cup league congress masters qualifier qualifiers qualifying main section group '
              'division final finals elite legendary legend'.split())
GENERIC.update('selection exchange challenge chess pro qualification individual pairs state city '
               'quarterly spring summer autumn winter lightning self-paired self-pair major official '
               'year parliamentary student students high school middle rapid speed strongest sports '
               'b b1 b2 c d s a rookies rookie dan kyu'.split())
EXCLUDED_PHRASES = {'china democratic league', 'democratic league'}
BAD.discard('new')  # Preserve geographic names such as New York.
PROTECTED = re.compile(r"(?:women['’]?s|youth|junior|senior|professional|amateur|pair|team)\s+$", re.I)


def tournament_key(text):
    folded = unicodedata.normalize('NFKD', text).casefold()
    return ''.join(c for c in folded if c.isalnum() and not unicodedata.combining(c))


def search_words(text):
    folded = unicodedata.normalize('NFKD', text).casefold()
    return re.findall(r'[^\W_]+', ''.join(c for c in folded if not unicodedata.combining(c)))


def clean_candidate(raw):
    words = raw.strip().split()
    # Narrative lead-ins can be title-cased. Keep only the noun phrase following
    # the last such word, not e.g. "Jane Wins the Paris Tournament".
    cut = max((i + 1 for i, w in enumerate(words[:-1]) if w.casefold() in BAD), default=0)
    words = words[cut:]
    while words and words[0].casefold() in ('the', 'a', 'an', 'of', 'in', 'for', 'and'):
        words.pop(0)
    name = ' '.join(words).strip(' ,:;–-')
    if name.casefold() in EXCLUDED_PHRASES:
        return None
    content = [w.casefold().strip("’'") for w in words if not re.fullmatch(NUMBER, w)]
    if len(name) > 180 or len(content) < 2:
        return None
    distinctive = [w for w in content if w.replace("'", '').replace('’', '') not in GENERIC
                   and w not in ('of', 'the', 'in', 'for', 'and', 'de', 'del', 'at')]
    # Geographic descriptions are still useful names, but unqualified "National
    # Championship"/"World Cup" must not merge unrelated countries and events.
    return name if distinctive else None


def extract_tournaments(title, body, source_id=''):
    fields = {'title_en': title or '', 'body_en': body or ''}
    found = {}

    def add(name, raw, field, start, end, method='phrase', series=False):
        key = tournament_key(name)
        if not key:
            return
        entry = found.setdefault(key, {'key': key, 'name': name, 'aliases': [], 'evidence': [], 'series': series})
        for alias in (*ALIASES.get(name, ()), raw if method != 'phrase' else name):
            if alias not in entry['aliases']:
                entry['aliases'].append(alias)
        if len(entry['evidence']) < 3:
            entry['evidence'].append({'field': field, 'start': start, 'end': end,
                                      'text': raw, 'method': method})

    for field, text in fields.items():
        spans = []
        # Publisher-provided EGD event rows in Polish ranking updates. A player
        # table has different columns; this pattern does not consume player names.
        if source_id == 'poland':
            for m in re.finditer(r'(?m)^[A-D]\s+[A-Z]{2}\s+[^,\n]+,\s*([^\n]+)', text):
                raw = m[1].strip()
                end = m.end(1)
                if re.search(r'\b(?:in|of|the|and|for)\s*$', raw):
                    continuation = re.match(r'\n([^\n|]+)', text[end:])
                    if continuation and continuation[1] != 'EGD' and len(continuation[1]) < 100:
                        end += len(continuation[0])
                        raw = text[m.start(1):end]
                if 4 < len(raw) <= 180:
                    name = ' '.join(raw.split())
                    base_name = re.sub(r'^\d+(?:st|nd|rd|th)\s+', '', name)
                    base_name = re.sub(r'\s*(?:-\s*)?20\d{2}$', '', base_name)
                    canonical = next((canonical for canonical, aliases in ALIASES.items()
                                      if any(tournament_key(base_name) == tournament_key(alias) for alias in aliases)), None)
                    add(canonical or name, raw, field, m.start(1), end, 'publisher-event-row', bool(canonical))
                    spans.append((m.start(), end))
        alias_pairs = sorted(((canonical, alias) for canonical, aliases in ALIASES.items()
                              for alias in aliases), key=lambda pair: -len(pair[1]))
        for canonical, alias in alias_pairs:
            # Acronyms are case sensitive; normal phrases accept case variants.
            flags = 0 if alias.isupper() else re.I
            for m in re.finditer(r'(?<!\w)' + re.escape(alias) + r'(?:s)?(?!\w)', text, flags):
                if any(a <= m.start() and m.end() <= b for a, b in spans):
                    continue
                preceding = PROTECTED.search(text[max(0, m.start()-25):m.start()])
                if preceding:
                    continue
                add(canonical, m[0], field, m.start(), m.end(), 'reviewed-alias', True)
                spans.append((m.start(), m.end()))
        for m in PHRASE.finditer(text):
            raw = m[0]
            name = clean_candidate(raw)
            if not name:
                continue
            if len(name.split()) > 7 or re.search(r'\b(?:and|for|in|at|of)\b', name):
                continue
            if re.search(r'\b(?:FIFA|Football|Soccer)\b', name, re.I):
                continue
            start = m.start()
            # Suppress a generic duplicate of a reviewed alias or event row;
            # qualifiers ending in Tournament remain separate mentions.
            overlap = [(a, b) for a, b in spans if start < b and a < m.end()]
            if overlap:
                if not re.search(r'\b(?:Qualifier|Qualification|Qualifying)\b', name) or 'Qualification' in name and any(
                        'Qualification' in e['name'] and any(v['start'] < m.end() and start < v['end']
                            for v in e['evidence'] if v['field'] == field) for e in found.values()):
                    continue
            # New names require either a title mention, explicit quotation, or
            # repetition. Unreviewed phrases are scoped to the publisher so
            # vague names cannot join unrelated national events across sources.
            quoted = start > 0 and text[start-1] in '"“‘'
            if field != 'title_en' and not quoted and (title + '\n' + body).count(name) < 2:
                continue
            add(name, text[start:m.end()], field, start, m.end())
        for event in ('Kisei', 'Meijin', 'Honinbo', 'Judan', 'Tengen', 'Oza', 'Gosei'):
            # A title after a player's name is not sufficient evidence of an event.
            pattern = rf'\b(?:\d+(?:st|nd|rd|th)\s+{event}\b|{event}\s+(?:title match|league|tournament|championship|challenge)\b)'
            for m in re.finditer(pattern, text, re.I):
                if PROTECTED.search(text[max(0, m.start()-25):m.start()]):
                    continue
                add(event, m[0], field, m.start(), m.end(), 'title-competition-context', True)
                # The contextual pattern subsumes a generic "51st Kisei
                # Tournament" rather than listing it twice in the directory.
                for key, entry in list(found.items()):
                    if not entry['series'] and re.fullmatch(rf'(?:\d+(?:st|nd|rd|th)\s+)?(?:Go\s+)?{event}\s+(?:Tournament|League)', entry['name']):
                        del found[key]
    for entry in found.values():
        if not entry['series']:
            entry['key'] = source_id + ':' + entry['key']
    return sorted(found.values(), key=lambda e: e['name'].casefold())


def annotate_tournaments(record):
    if record.get('status') != 'ready':
        return record
    title = record.get('title_en') or record.get('title') or ''
    body = record.get('body_en') or ''
    digest = hashlib.sha256((VERSION + '\0' + record.get('source_id', '') + '\0' + title + '\0' + body).encode()).hexdigest()
    if record.get('tournaments_hash') != digest:
        try:
            record['tournaments'] = extract_tournaments(title, body, record.get('source_id', ''))
            record['tournaments_hash'] = digest
            record['tournaments_version'] = VERSION
            record.pop('tournaments_error', None)
        except Exception as error:
            record['tournaments'] = []
            record.pop('tournaments_hash', None)
            record['tournaments_error'] = f'{type(error).__name__}: {str(error)[:150]}'
    return record
