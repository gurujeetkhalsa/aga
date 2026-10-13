"""Locally recognized people, cached with the complete English article text."""
from collections import Counter
from functools import lru_cache
import hashlib
import re
import unicodedata

VERSION = 'people-v2'
_WORD = r"[A-ZÀ-ÖØ-Þ][a-zA-ZÀ-ÖØ-öø-ÿ]+(?:[-’'][A-Za-zÀ-ÖØ-öø-ÿ]+)*"
_RANKED = re.compile(r'\b(' + _WORD + r'(?:\s+' + _WORD + r'){1,3})\s+(?:[1-9]\s*(?:[dpk]\b|[- ]?dan\b|[- ]?kyu\b))')
_ACTION = re.compile(r'\b(' + _WORD + r'(?:\s+' + _WORD + r'){1,2})(?:\s*[^\w\s,]{1,5})?\s+(?:(?:has|had)\s+(?:been\s+)?)?(?:won|wins|defeated|defeats|beat|promoted|earned|finished|clinched)\b')
_NON_PERSON = set('go weiqi baduk tournament championship champion association federation congress cup league world international national open amateur professional university school hotel club team round game games tournament committee news journal japan china korea chinese japanese korean european europe asia asian north south american british german french taiwan thailand singapore united states city new york prime minister president director secretary general september october november december january february march april may june july august monday tuesday wednesday thursday friday saturday sunday copyright read more click here black white event winner winners qualifying finals invitational selection'.split())
_NON_PERSON.update('wins prize matches insurance province island lighthouse fresh sauce liquor group regulations management ceremony shrine inauguration works outing warfare ward benyin guru ilbo played players player dan kyu dream'.split())
_NON_PERSON.update('nihon kansai ki-in ki- kindergarten division represent india asahi shimbun broadcast schedule road headquarters college conference format hotel grand jeonnam gwangju video youtube channel dojang discord sensei ave makes promoted elementary branch returns contact runner updated upset upsets extends held online workshop title cyber republic deluxe strongest page after winner runner-up receives earns leads celebrates announces first second third fourth fifth'.split())
_NON_PERSON.difference_update({'go', 'white', 'black', 'may'})
_EXCLUDED = {'cartagenadeindias', 'buanredsunset', 'farandagrand', 'chiyuan', 'denbosch'}


def name_key(name):
    text = unicodedata.normalize('NFKD', name).casefold()
    return ''.join(c for c in text if c.isalpha() and not unicodedata.combining(c))


def clean_name(name):
    name = re.sub(r'\s+', ' ', name).strip()
    name = re.sub(r"^(?:China|Korea|Japan|Buan)['’]s\s+", '', name)
    name = re.sub(r"['’]s\b", '', name)
    name = re.sub(r'\s+(?:[1-9]\s*(?:[dpk]|[- ]?dan|[- ]?kyu)|\d{6,})\b.*$', '', name)
    name = re.sub(r'^(?:rookie|dan)\s+', '', name)
    name = re.sub(r'\s+(?:Honinbo|Meijin|Kisei|Oza|Judan|Tengen|Wins|Makes|Promoted|Extends|Updated|Contact|Runner)$', '', name)
    name = re.sub(r'^(?:Mr\.?|Mrs\.?|Ms\.?|Dr\.?)\s+', '', name).strip(' ,.:;')
    words = name.split()
    if not 2 <= len(words) <= 4 or len(name) > 80:
        return None
    if name_key(name) in _EXCLUDED or any(w.casefold().strip('.,') in _NON_PERSON for w in words):
        return None
    if not all(w[0].isalpha() and all(c.isalpha() or c in ".’'-" for c in w) and not w.endswith('-') for w in words):
        return None
    return name


@lru_cache(maxsize=1)
def recognizer():
    import spacy
    return spacy.load('en_core_web_sm', exclude=['parser', 'lemmatizer', 'tagger', 'attribute_ruler', 'senter'])


def extract_people(text):
    nlp = recognizer()
    found = Counter()
    # Every part of the full text is processed; bounded chunks keep memory low.
    chunks = []
    rest = text
    while len(rest) > 20000:
        end = rest.rfind('\n', 10000, 20000)
        if end < 0:
            end = rest.rfind(' ', 10000, 20000)
        if end < 0:
            end = 20000
        chunks.append(rest[:end]); rest = rest[end:]
    chunks.append(rest)
    for doc in nlp.pipe(chunks, batch_size=4):
        for entity in doc.ents:
            candidate = clean_name(entity.text) if entity.label_ == 'PERSON' else None
            if candidate:
                found[candidate] += 1
    for sentence in text.splitlines():
        # Player ranks add reliable domain evidence for names missed by NER.
        for match in list(_RANKED.finditer(sentence)) + list(_ACTION.finditer(sentence)):
            candidate = clean_name(match[1])
            if candidate:
                found[candidate] += 1
    variants = {}
    for name, count in found.items():
        key = name_key(name)
        if key not in variants or count > variants[key][1]:
            variants[key] = (name, count)
    return [{'key': key, 'name': value[0]} for key, value in sorted(variants.items())]


def annotate_people(record):
    if record.get('status') != 'ready':
        return record
    text = (record.get('title_en') or record.get('title') or '') + '\n' + (record.get('body_en') or '')
    digest = hashlib.sha256((VERSION + '\0' + text).encode()).hexdigest()
    if record.get('people_hash') != digest:
        try:
            record['people'] = extract_people(text)
            record['people_hash'] = digest
            record['people_version'] = VERSION
            record.pop('people_error', None)
        except Exception as error:
            # Name recognition must never discard searchable article text.
            record['people'] = []
            record.pop('people_hash', None)
            record['people_error'] = f'{type(error).__name__}: {str(error)[:150]}'
    return record
