
from models import Author, Tag, Quote
from datetime import datetime

# Authors
einstein = {
    'name': 'Albert Einstein',
    'birthday': datetime(month=3, day=14, year=1879),
    'bio': 'Won the 1921 Nobel Prize in Physics.'
}
kennedy = {
    'name': 'John F. Kennedy',
    'birthday': datetime(month=5, day=29, year=1917),
    'bio': '35th president of the United States.'
}

einstein_instance = Author(**einstein)
kennedy_instance = Author(**kennedy)

# Tags
change = {'name': 'change'}
community = {'name': 'community'}
deep_thoughts = {'name': 'deep-thoughts'}
inspirational = {'name': 'inspirational'}

change_instance = Tag(**change)
community_instance = Tag(**community)
deep_thoughts_instance = Tag(**deep_thoughts)
inspirational_instance = Tag(**inspirational)

# Quotes
einstein_quote_I = {
    'quote': (
        'The world as we have created it is a process of our thinking. '
        'It cannot be changed without changing our thinking.'
    ),
    'author': einstein_instance,
    'tags': [change_instance, deep_thoughts_instance]
}
einstein_quote_II = {
    'quote': (
        'There are only two ways to live your life. '
        'One is as though nothing is a miracle. '
        'The other is as though everything is a miracle.'
    ),
    'author': einstein_instance,
    'tags': [inspirational_instance]
}
kennedy_quote_I = {
    'quote': 'If not us, who? If not now, when?',
    'author': kennedy_instance,
    'tags': [change_instance, deep_thoughts_instance]
}
kennedy_quote_II = {
    'quote': (
        'Ask not what your country can do for you, '
        'but what you can do for your country.'
    ),
    'author': kennedy_instance,
    'tags': [community_instance]
}

einstein_quote_I_instance = Quote(**einstein_quote_I)
einstein_quote_II_instance = Quote(**einstein_quote_II)
kennedy_quote_I_instance = Quote(**kennedy_quote_I)
kennedy_quote_II_instance = Quote(**kennedy_quote_II)
