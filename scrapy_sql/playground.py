
# relationships = self.relationships
# for field_name in relationships:

#     print(
#         f'\nSETTING RELATIONSHIPS FOR {self.__class__} {field_name}\n')

#     try:
#         filter_kwargs = getattr(self, f'__{field_name}_query_filter')
#         input(filter_kwargs)
#     except AttributeError:
#         input(f'NO MATCH FOR {self.__class__} {field_name}\n')
#         continue

#     mapper_cls, _, targets = relationships.get(field_name).values()

#     # targets will be an instance of InstrumentedList,
#     # Table, DeclarativeMeta or NoneType
#     if targets is None:
#         print('targets is None')
#         print(f"{mapper_cls=}")
#         print(f"{filter_kwargs=}")
#         print(f"{new_value=}")
#     elif isinstance(targets, (Table, DeclarativeMeta)):
#         print('Target already set')
#         # new_value = existing_table(session, targets)
#     elif isinstance(targets, InstrumentedList):
#         print('Multiple Targets')

#     print(f'{field_name} sat to {new_value}')

#     setattr(self, field_name, new_value)
