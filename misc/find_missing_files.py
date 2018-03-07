from glob import glob
from os.path import join, split, splitext

# pattern = join('data', 'DOE-nitfix_specimen_photos', '*.JPG')
pattern = join('data', 'HUH_DOE-nitfix_specimen_photos', '*.JPG')


numbers = []
for path in glob(pattern):
    _, file_name = split(path)
    name, _ = splitext(file_name)
    number = int(name[1:])
    numbers.append(number)
    # print(number)

min_number = min(numbers)
max_number = max(numbers)

actual = set(numbers)
expect = set([i for i in range(min_number, max_number + 1)])

# print(min_number)
# print(max_number)
missing = expect - actual
# print(len(missing))

file_names = [f'R{n:07}' for n in missing]
for file_name in sorted(file_names):
    print(file_name)
