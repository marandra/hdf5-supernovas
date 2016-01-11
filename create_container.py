import sys
import h5py
import numpy as np
import glob
import os
import ConfigParser

description = '''
description: reads an attributes file from arguments.
       If argument 'global' is missing, it will try to import only datasets,
       all but the last one in case it is open. With the 'global' option it
       will load all the datasets and the globab files.

atrributes file structure:
       [global dataset name]
       filename =
       type = table, text, dataset
       Description = Description of the table
       Column 00 = description
       ...
       [global dataset name 2]

container structure:
/output/
        parameters
        luminosity
        stabilization
        bounce
        conservation
        Luminosity
        datasets/
                 000100/
                        attr: Iteration/Time
                        dynamics
                        thermodynamics
                        Neutrinos
                        SPH
                 000200/
                  ...
'''  


def import_text(fh5, datasetname, fn, attr):
    if fh5.__contains__(datasetname):
        fh5.__delitem__(datasetname)
        print("Updated previous " + datasetname)
    with open(fn, 'r') as f:
        lf = f.read()
    #if datasetname == '/output/bounce':
    #    data = np.array([('iteration', int(lf[1].split()[0])),
    #                     ('time',      float(lf[1].split()[1]))],
    #                     dtype=[('key', '|S9'),('value', 'f4')])
    #    lf = data
    dset = fh5.create_dataset(datasetname, data=lf)
    for k, v in attr.items():
        dset.attrs[k] = v

def import_table(fh5, datasetname, fn, attr):
    if fh5.__contains__(datasetname):
        fh5.__delitem__(datasetname)
        print("Updated previous " + datasetname)
    with open(fn, 'r') as f:
        lf = list(f)
    nfields = len(attr) - 1
    data = [map(float, x.split()[:nfields]) for x in lf]
    dset = fh5.create_dataset(datasetname, data=data, compression=True)
    for k, v in attr.items():
        dset.attrs[k] = v


def parse_datasets(attr):
    la = [[k, v] for k, v in attr.items()]
    la.sort()
    field = [x[1].split(':')[0].strip() for x in la]
    ld = [x[1].split(':')[1] for x in la]
    rhs = [x[1].split(':')[1].split(',') for x in la]
    dataset = {}
    for d in set([y.strip() for li in rhs for y in li]):
        dataset[d] = [i for i, x in enumerate(ld) if d in x]
    return field, dataset


def import_data(fh5, path, iter, fnl, timelist, attr, datasets):
    group = os.path.join(path, iter)
    if fh5.__contains__(group):
        print("Iteration " + group + " exists")
        return
    #create group and attribute
    grp = fh5.create_group(group)
    grp.attrs['Iteration'] = int(iter)
    grp.attrs['Time'] = timelist[int(iter)]
    #read files, populate datasets
    data = []
    for fn in fnl:
        with open(fn, 'r') as f:
            data.extend(list(f))
    for ds, idx in datasets.items():
        dv = np.zeros((len(data), len(idx)))
        for line in data:
            field = line.split()
            part = int(field[0])
            dv[part-1] = [float(field[i]) for i in idx]
        #create dset and attr
        dset = grp.create_dataset(ds, data=dv, compression=True)
        dset.attrs['Row'] = 'Particle (first one is 0, originally 1)'
        for i, id in enumerate(idx):
            dset.attrs['Column {:02d}'.format(i)] = attr[id]


#################    
if len(sys.argv) < 2:
    print("usage: python {} file [global]".format(sys.argv[0]))
    print(description)
    sys.exit()

# get arguments
lpath = sys.argv[1].rsplit('/', 1)
fn_attr = lpath.pop()
path = lpath.pop() if lpath else ''   
flglobal = False
if len(sys.argv) > 2 and sys.argv[2] == 'global':
    flglobal = True
# get attributes from file
parser = ConfigParser.ConfigParser(allow_no_value=True)
parser.optionxform = str
parser.read(os.path.join(path, fn_attr))
attrs = {}
for s in parser.sections():
    attrs[s] = {o: parser.get(s, o) for o in parser.options(s)}
# open/create h5 file and iterate over tables and datasets in attributes file
with h5py.File('output.h5', 'a') as fh5:
    for field, att in attrs.items():
        type = att.pop('type')
        fn = os.path.join(path, att.pop('filename'))
        if type == 'table' and flglobal:
            import_table(fh5, '/output/Global/' + field, fn, att)
        elif type == 'text' and flglobal:
            import_text(fh5, '/output/Global/' + field, fn, att)
        elif type == 'dataset':
            tfn = os.path.join(path, parser.get('Iteration_Time', 'filename'))
            with open(tfn, 'r') as f:
                itertime = {int(x.split()[0]): float(x.split()[1]) for x in list(f)}
            fields, dsnames = parse_datasets(att)
            fl = glob.glob(fn)
            fi = set(['.'.join(f.split('/')[-1].split('.')[:2]) for f in fl])
            l = [glob.glob(os.path.join(path, f + '*')) for f in fi if int(f.split('.')[1]) in itertime]
            #skips last one in case files are not closed just yet
            if not flglobal and l:
                l.pop()
            for ds in l:
                i = ds[0].rsplit('/')[-1].split('.')[1]
                print('Importing iteration: {}'.format(i))
                import_data(fh5, 'output/' + field, i, ds, itertime, fields, dsnames)


