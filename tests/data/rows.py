
from models import *
from datetime import datetime

change = Tag(**{'name': 'change'})
deep_thoughts = Tag(**{'name': 'deep-thoughts'})
inspirational = Tag(**{'name': 'inspirational'})
community = Tag(**{'name': 'community'})

einstein = Author(**{
    'name': 'Albert Einstein',
    'birthday': datetime(month=3, day=14, year=1879),
    'bio': 'Won the 1921 Nobel Prize in Physics.'
})
kennedy = Author(**{
    'name': 'John F. Kennedy',
    'birthday': datetime(month=5, day=29, year=1917),
    'bio': '35th president of the United States.'
})

einstein_quote_I = Quote(**{
    'quote': (
        'The world as we have created it is a process of our thinking. '
        'It cannot be changed without changing our thinking.'
    ),
    'author': einstein,
    'tags': [change, deep_thoughts]
})
einstein_quote_II = Quote(**{
    'quote': (
        'There are only two ways to live your life. '
        'One is as though nothing is a miracle. '
        'The other is as though everything is a miracle.'
    ),
    'author': einstein,
    'tags': [inspirational]
})

kennedy_quote_I = Quote(**{
    'quote': 'If not us, who? If not now, when?',
    'author': kennedy,
    'tags': [change, deep_thoughts]
})
kennedy_quote_II = Quote(**{
    'quote': (
        'Ask not what your country can do for you, '
        'but what you can do for your country.'
    ),
    'author': kennedy,
    'tags': [community]
})
