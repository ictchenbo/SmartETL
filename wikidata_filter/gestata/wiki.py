#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) 2016-2017 emijrp <emijrp@gmail.com>
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import datetime
import os
import re
import sys
import unicodedata

# ideas:
# que ponga icono de PDF a los enlaces PDF (a veces se hace pesado en páginas con muchos pdf)
# mostrar icono de Wayback Machine al hacer hover sobre enlace
# que genere una galeria de archivos con todo el directorio images
# generar jpg con la portada de los pdf


def log(error=False, msg=""):
    if error:
        with open("error.log", "a") as f:
            msg = "%s\n" % (msg)
            f.write(msg.encode("utf-8"))


def readwikifile(path, wikifile):
    fullpath = '%s/%s' % (path, wikifile)
    if os.path.exists(fullpath):
        f = open(fullpath, 'r')
        wiki = unicode(f.read(), 'utf-8').strip()
        f.close()
    else:
        wiki = u'<span style="text-color: #FF0000;">[Página %s no encontrada]</span>' % (fullpath)
        log(error=True, msg="%s not found" % (fullpath))
    
    return wiki

def savehtmlfile(path, htmlfile, html):
    fullpath = '%s/%s' % (path, htmlfile)
    f = open(fullpath, 'w')
    f.write(html.encode('utf-8'))
    f.close()

def includes(wiki, wikifile):
    wiki = re.sub(r'(?im)<noinclude>(.*?)</noinclude>', r'\1', wiki)
    wiki = re.sub(r'(?im)<includeonly>.*?</includeonly>', r'', wiki)
    
    return wiki

def sections(wiki, wikifile):
    wiki = re.sub(r'(?im)^=\s*([^=]+?)\s*=', r'<h1>\1</h1>', wiki)
    wiki = re.sub(r'(?im)^==\s*([^=]+?)\s*==', r'<h2 id="\1">\1</h2>', wiki)
    wiki = re.sub(r'(?im)^===\s*([^=]+?)\s*===', r'<h3 id="\1">\1</h3>', wiki)
    wiki = re.sub(r'(?im)^====\s*([^=]+?)\s*====', r'<h4 id="\1">\1</h4>', wiki)
    m = re.findall(r'(?im)<h([234]) id="([^<>]*?)">', wiki)
    for i in m:
        wiki = wiki.replace('<h%s id="%s">' % (i[0], i[1]), '<h%s id="%s">' % (i[0], re.sub(' ', '_', i[1])))
    
    return wiki

def templates(wiki, path, wikifile):
    templatetypes = [
        [r'(?im)\{\{(\:[^\{\}]+?)\}\}', path], # templates in the same directory, prefix :
        [r'(?im)\{\{([^\{\}]+?)\}\}', './templates'], # global templates
    ]
    for templateregexp, templatepath in templatetypes:
        templates = re.findall(templateregexp, wiki)
        """if os.path.exists('%s/rootpath.wiki' % (path)):
            f = open('%s/rootpath.wiki' % (path))
            templatepath = f.read().strip() + '/templates'
            f.close()"""
        
        for template in templates:
            templatename = template.split('|')[0].split('\n')[0].strip()
            templateparameters = template.split('|')[1:]
            templatefilename = re.sub(' ', '-', templatename.strip(':').lower())
            wikitemplate = readwikifile(templatepath, '%s.wiki' % templatefilename)
            # remove noinclude
            wikitemplate = re.sub(r'(?im)<noinclude>.*?</noinclude>', r'', wikitemplate)
            # clean includeonly
            wikitemplate = re.sub(r'(?im)<includeonly>(.*?)</includeonly>', r'\1', wikitemplate)
            c = 1
            for templateparameter in templateparameters:
                parametername = templateparameter.split('=')[0].strip()
                try:
                    parametervalue = templateparameter.split('=')[1].strip()
                except:
                    parametervalue = u''
                wikitemplate = wikitemplate.replace('{{{%s}}}' % (parametername), parametervalue)
                wikitemplate = wikitemplate.replace('{{{%s|}}}' % (parametername), parametervalue)
                c += 1
            #remove empty parameters when {{{X|}}}
            wikitemplate = re.sub(r'{{{\d+\|\s*?}}}', '', wikitemplate)
            
            #recursive template replacing
            htmltemplate = wiki2html(wikitemplate, path, wikifile)
            wiki = wiki.replace('{{%s}}' % (template), htmltemplate)
    
    return wiki


def images(wiki, path, wikifile):
    imagepath = './images'
    if os.path.exists('%s/rootpath.wiki' % (path)):
        f = open('%s/rootpath.wiki' % (path))
        imagepath = f.read().strip() + '/images'
        f.close()
    
    images = re.findall(r'(?im)\[\[File:([^\[\]]+?)\]\]', wiki)
    for image in images:
        #print image
        imagename = image.split('|')[0]
        imageparameters = image.split('|')[1:]
        imagewidth = ''
        imageposition = 'right'
        imagefooter = False
        imagelink = '%s/%s' % (imagepath, imagename)
        imagethumblink = imagelink
        imagedesc = imagename
        for imageparameter in imageparameters:
            imageparameter = imageparameter.strip()
            
            if re.search(r'\d+px', imageparameter):
                imagewidth = imageparameter
                imagewidth_ = imageparameter.split('px')[0]
            elif re.search(r'(?i)(left|center|right)', imageparameter):
                imageposition = imageparameter
            elif re.search(r'(?i)(thumb)', imageparameter):
                imagefooter = True
            elif re.search(r'(?i)link=', imageparameter):
                t = imageparameter.split('link=')[1].strip()
                imagelink = t or imagelink
            else:
                imagedesc = imageparameter
        if imagewidth:
            imagethumblink = '%s/thumbs/%s-%s' % (imagepath, imagewidth, imagename)
            if os.path.exists("images/%s" % (imagename)):
                if os.path.exists('images/thumbs/%s-%s' % (imagewidth, imagename)):
                    print("    Thumb exists images/thumbs/%s-%s Skiping..." % (imagewidth, imagename))
                else:
                    print("    Resizing images/%s into images/thumbs/%s-%s" % (imagename, imagewidth, imagename))
                    os.system("convert images/%s -resize %sx%s images/thumbs/%s-%s" % (imagename, imagewidth_, imagewidth_, imagewidth, imagename))
            else:
                print("    No exists images/%s" % (imagename))
        
        if imagename.endswith('.pdf'):
            #wiki = wiki.replace('[[File:%s]]' % (image), '<a href="%s/%s">%s</a> (PDF)' % (imagepath, imagename, imagedesc))
            wiki = wiki.replace('[[File:%s]]' % (image), '<a href="%s">%s</a>' % (imagelink, imagedesc))
        else:
            if imagefooter:
                wiki = wiki.replace('[[File:%s]]' % (image), '<div width="%s" style="float: %s;padding: 3px;"><table width="%s" style="float: %s;border: 1px solid lightgrey;padding: 0px;"><tr><td><a href="%s"><img src="%s" width="%s" align="%s" alt="%s" title="%s" /></a></td></tr><tr><td style="font-size: 90%%;">%s</td></tr></table></div>' % (imagewidth, imageposition, imagewidth, imageposition, imagelink, imagethumblink, imagewidth, imageposition, imagedesc, imagedesc, imagedesc))
            else:
                wiki = wiki.replace('[[File:%s]]' % (image), '<a href="%s"><img src="%s" width="%s" align="%s" alt="%s" title="%s" /></a>' % (imagelink, imagethumblink, imagewidth, imageposition, imagedesc, imagedesc))
    
    return wiki


def paragraphs(wiki, wikifile):
    paragraphs = wiki.strip().split('\n')
    wiki2 = ''
    skipline = False
    for paragraph in paragraphs:
        paragraph2 = paragraph.strip()
        if skipline:
            if '</script>' in paragraph2 or \
                '-->' in paragraph2 or \
                '</pre>' in paragraph2 or \
                '</ul>' in paragraph2 or \
                '</ol>' in paragraph2:
                wiki2 += '%s\n' % (paragraph)
                skipline = False
            else:
                wiki2 += '%s\n' % (paragraph)
            continue
        if paragraph2 == '':
            wiki2 += '\n'
        elif paragraph2.startswith('*') or paragraph2.startswith('#'):
            wiki2 += '%s\n' % (paragraph)
            continue
        elif paragraph2.startswith('<'):
            if ('<script' in paragraph2 and not '</script>' in paragraph2) or \
                ('<!--' in paragraph2 and not '-->' in paragraph2) or \
                ('<pre' in paragraph2 and not '</pre>' in paragraph2) or \
                ('<ul>' in paragraph2 and not '</ul>' in paragraph2) or \
                ('<ol>' in paragraph2 and not '</ol>' in paragraph2):
                wiki2 += '%s\n' % (paragraph)
                skipline = True
                continue
            wiki2 += '%s\n' % (paragraph)
        else:
            if len(paragraphs) > 1: #only add paragraphs for wikitexts longer than 1 paragraph
                wiki2 += '<p>%s</p>\n' % (paragraph)
            else: #avoid break lines
                wiki2 += '%s' % (paragraph)
    wiki = wiki2
    
    return wiki


def textformat(wiki, wikifile):
    wiki = re.sub(r'(?im)\'{3}([^\']+?)\'{3}', r'<b>\1</b>', wiki)
    wiki = re.sub(r'(?im)\'{2}([^\']+?)\'{2}', r'<i>\1</i>', wiki)
    #wiki = re.sub(r'(?im)_([^\_]+?)_', r'<u>\1</u>', wiki) # error: reemplaza los _ de las urls
    wiki = re.sub(r'(?im)^:', '&nbsp;&nbsp;&nbsp;&nbsp;', wiki)
    
    return wiki


def removeaccute(s):
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    return s


def linksinternal(wiki, wikifile):
    wiki = re.sub(r'(?i)\[\[(%s)\|([^\]]*?)\]\]' % (wikifile.split('.wiki')[0].lower()), r'<b>\2</b>', wiki)
    wiki = re.sub(r'(?i)\[\[(%s)\]\]' % (wikifile.split('.wiki')[0].lower()), r'<b>\1</b>', wiki)
    
    #[[link|links]]
    m = re.findall(r'(?im)\[\[([^\[\]\|]+?)\|([^\[\]\|]+?)\]\]', wiki)
    for i in m:
        if i[0].startswith('#'):
            wiki = re.sub(r'(?im)\[\[%s\|%s\]\]' % (i[0], i[1]), '<a href="%s">%s</a>' % (i[0], i[1]), wiki)
        elif '#' in i[0]:
            wiki = re.sub(r'(?im)\[\[%s\|%s\]\]' % (i[0], i[1]), '<a href="%s.html#%s">%s</a>' % (i[0].split('#')[0], i[0].split('#')[1], i[1]), wiki)
        else:
            wiki = re.sub(r'(?im)\[\[%s\|%s\]\]' % (i[0], i[1]), '<a href="%s.html">%s</a>' % (re.sub(' ', '-', i[0].lower()), i[1]), wiki)
    
    #[[link]]
    m = re.findall(r'(?im)\[\[([^\[\]\|]+?)\]\]', wiki)
    for i in m:
        if i.startswith('#'):
            wiki = re.sub(r'(?im)\[\[%s\]\]' % (i), '<a href="%s">%s</a>' % (i, i), wiki)
        elif '#' in i:
            wiki = re.sub(r'(?im)\[\[%s\]\]' % (i), '<a href="%s.html#%s">%s</a>' % (i.split('#')[0], i.split('#')[1], i), wiki)
        else:
            wiki = re.sub(r'(?im)\[\[%s\]\]' % (i), '<a href="%s.html">%s</a>' % (re.sub(' ', '-', removeaccute(i.lower())), i), wiki)
        
    return wiki


def linksexternal(wiki, wikifile):
    # PDF #buscar icono y quitar lo de PDF
    #wiki = re.sub(r'(?im)\[((?:https?://|ftps?://|\./)[^\[\]\| ]+?\.pdf)\s+([^\[\]\|]+?)\]', r'<a href="\1">\2</a> (PDF)', wiki)
    wiki = re.sub(r'(?im)\[((?:https?://|ftps?://|\./)[^\[\]\| ]+?\.pdf)\s+([^\[\]\|]+?)\]', r'<a href="\1">\2</a>', wiki)
    wiki = re.sub(r'(?im)\[((?:https?://|ftps?://|\./)[^\[\]\| ]+?\.pdf)\]', r'<a href="\1">\1</a>', wiki)
    
    # other
    wiki = re.sub(r'(?im)\[((?:https?://|ftps?://|\./)[^\[\]\| ]+?)\s+([^\[\]\|]+?)\]', r'<a href="\1">\2</a>', wiki)
    wiki = re.sub(r'(?im)\[((?:https?://|ftps?://|\./)[^\[\]\| ]+?)\]', r'<a href="\1">\1</a>', wiki)
    
    return wiki

def references(wiki, wikifile):
    refs = {}
    m = re.findall(r'(?im)(<ref( name=["\']([^<>]+?)["\'])?>(.*?)</ref>)', wiki)
    c = 1
    for i in m:
        #print i
        ref = i[0]
        refname = i[2]
        refcontent = i[3]
        refnum = c
        if refname:
            refs[refname] = refnum
        wiki = wiki.replace(ref, '<sup>[<a id="refback%s" href="#ref%s">%s</a>]</sup>' % (c, c, c))
        wiki = wiki.replace('<!--/references-->', u'<li id="ref%s"><a href="#refback%s">↑</a> %s</li>\n<!--/references-->' % (c, c, refcontent))
        c += 1
    
    m = re.findall(r'(?im)(<ref( name=["\']([^<>]+?)["\'])?\s*/\s*>)', wiki)
    for i in m:
        #print i
        ref = i[0]
        refname = i[2]
        if refname in refs:
            wiki = wiki.replace(ref, '<sup>[<a href="#ref%s">%s</a>]</sup>' % (refs[refname], refs[refname]))
    
    return wiki

def itemlist(wiki, wikifile):
    c = 10
    while c > 0:
        wiki = re.sub(r'(?im)^%s *([^\n]*?)$' % ('\*'*c), r'%s<li>\1</li>%s' % ('<ul>'*c, '</ul>'*c), wiki)
        c -= 1
    c = 10
    while c > 0:
        wiki = re.sub(r'(?im)%s\n*%s' % ('</ul>'*c, '<ul>'*c), '', wiki)
        c -= 1
    
    c = 10
    while c > 0:
        wiki = re.sub(r'(?im)^%s *([^\n]*?)$' % ('\#'*c), r'%s<li>\1</li>%s' % ('<ol>'*c, '</ol>'*c), wiki)
        c -= 1
    c = 10
    while c > 0:
        wiki = re.sub(r'(?im)%s\n*%s' % ('</ol>'*c, '<ol>'*c), '', wiki)
        c -= 1
    
    return wiki

def indexrootpath(wiki, path, wikifile):
    indexrootpath = '.'
    if os.path.exists('%s/rootpath.wiki' % (path)):
        f = open('%s/rootpath.wiki' % (path))
        indexrootpath = f.read().strip() + '/index'
        f.close()
    pathtofile = '%s/%s' % (path, wikifile)
    if os.path.exists(pathtofile):
        wiki = re.sub('<!-- indexrootpath -->.*?<!-- /indexrootpath -->', '%s' % (indexrootpath), wiki)
    
    return wiki

def timestamp(wiki, path, wikifile):
    pathtofile = '%s/%s' % (path, wikifile)
    if os.path.exists(pathtofile):
        dt = datetime.datetime.utcfromtimestamp(os.path.getmtime(pathtofile))
        wiki = re.sub('<!-- timestamp -->.*?<!-- /timestamp -->', '%s' % (dt.strftime('%Y-%m-%d %H:%M:%S')), wiki)
    
    return wiki

def toc(wiki, wikifile):
    if '__notoc__' in wiki.lower():
        wiki = re.sub(r'(?im)__NOTOC__', '', wiki)
        return wiki
    if 'id="toc"' in wiki:
        return wiki
    
    m = re.findall(r'(?im)<h([234]) id="([^<>]*?)">([^<>]*?)</h[234]>', wiki)
    l2 = 0
    l3 = 0
    l4 = 0
    if len(m) >= 2:
         toc = '<table id="toc" class="wikitable">\n'
         toc += '<tr><th>Tabla de contenidos</th></tr>\n'
         toc += '<tr><td>\n'
         for i in m:
             if int(i[0]) == 2:
                 l2 += 1
                 l3 = 0
                 l4 = 0
                 toc += '&nbsp;%s. <a href="#%s">%s</a><br/>\n' % (l2, i[1], i[2])
             elif int(i[0]) == 3:
                 l3 += 1
                 l4 = 0
                 toc += '&nbsp;&nbsp;&nbsp;&nbsp;%s.%s <a href="#%s">%s</a><br/>\n' % (l2, l3, i[1], i[2])
             elif int(i[0]) == 4:
                 l4 += 1
                 toc += '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%s.%s.%s <a href="#%s">%s</a><br/>\n' % (l2, l3, l4, i[1], i[2])
         
         toc += '</td></tr>\n'
         toc += '</table>\n'
         wiki = re.sub('<h2', '%s\n<h2' % (toc), wiki, count=1)
    
    return wiki

def sitemap(wikilist):
    if not os.path.exists('sitemap.wiki'):
        return 
    
    wikilist.sort()
    table = u'\n<script type="text/javascript">sorttable.sort_alpha = function(a,b) { return a[0].localeCompare(b[0], "es"); }</script>\n'
    table += u'\n<table class="wikitable sortable" style="text-align: center;">\n'
    table += u"""
    <tr>
        <th class="sorttable_numeric">#</th>
        <th class="sorttable_alpha">Página HTML</th>
        <th class="sorttable_alpha">Página wiki</th>
        <th class="sorttable_numeric">Tamaño (bytes)</th>
    </tr>"""
    sitemaprows = []
    c = 1
    for path, filename in wikilist:
        filename2 = filename.split('.wiki')[0]
        row = u"""
    <tr>
        <td>%s</td>
        <td><a href="%s/%s.html">%s/%s.html</a></td>
        <td><a href="%s/%s.wiki">%s/%s.wiki</a></td>
        <td>%d</td>
    </tr>\n""" % (c, path, filename2, path, filename2, path, filename2, path, filename2, len(readwikifile(path, filename)))
        sitemaprows.append(row)
        c += 1
    
    table += u''.join(sitemaprows)
    table += u'</table>\n'
    
    f = open('sitemap.wiki', 'r')
    wikicode = unicode(f.read(), 'utf-8')
    f.close()
    f = open('sitemap.wiki', 'w')
    wikicode = u'%s<!-- tabla completa -->%s<!-- /tabla completa -->%s' % (wikicode.split(u'<!-- tabla completa -->')[0], table, wikicode.split(u'<!-- /tabla completa -->')[1])
    f.write(wikicode.encode('utf-8'))
    f.close()

def search(wiki, path, wikifile):
    entry = []
    
    m = re.findall(ur'(?im)\{\{\s*header[^\{\}\|\n\r]*?\s*\|\s*(1=)?\s*([^\{\}\|\n\r]+?)\s*\}\}', wiki)
    if m and m[0][1]:
        header = m[0][1]
        keywords = set([])
        keywords |= set([x[0].lower() for x in re.findall(ur'([A-ZÁÉÍÓÚÀÈÌÒÙÑÇ][A-Za-zÁÉÍÓÚÀÈÌÒÙÑÇáéíóúàèìòùñç\-]{1,}([ \.][A-ZÁÉÍÓÚÑ][a-záéíóúñ]{1,})*)', wiki)])
        keywords |= set([x.lower() for x in re.findall(ur'([A-Za-zÁÉÍÓÚÀÈÌÒÙÑÇáéíóúàèìòùñç\-]{3,})', wiki)])
        keywords.add(header.lower())
        keywords = list(keywords)
        keywords.sort()
        #print keyterms
        entry = [header, path, wikifile.split('.wiki')[0], keywords]
    
    return entry

def relpathcssjs(html, path):
    rootpath = '.'
    if os.path.exists('%s/rootpath.wiki' % (path)):
        f = open('%s/rootpath.wiki' % (path))
        rootpath = f.read().strip()
        f.close()
    rootpath += '/'
    html = re.sub('<link rel="stylesheet" href="', '<link rel="stylesheet" href="%s' % (rootpath), html)
    html = re.sub('<script src="', '<script src="%s' % (rootpath), html)
    return html

def wiki2html(wiki, path, wikifile):
    wiki = indexrootpath(wiki, path, wikifile)
    wiki = includes(wiki, wikifile)
    wiki = sections(wiki, wikifile)
    wiki = templates(wiki, path, wikifile)
    wiki = images(wiki, path, wikifile)
    wiki = paragraphs(wiki, wikifile)
    wiki = references(wiki, wikifile)
    wiki = textformat(wiki, wikifile)
    wiki = linksinternal(wiki, wikifile)
    wiki = linksexternal(wiki, wikifile)
    wiki = itemlist(wiki, wikifile)
    wiki = toc(wiki, wikifile)
    wiki = timestamp(wiki, path, wikifile)
    
    html = wiki
    return html

def processfile(path, wikifile):
    wiki = readwikifile(path, wikifile)
    html = wiki2html(wiki, path, wikifile)
    html = relpathcssjs(html, path)
    #print(html)
    htmlfile = '%s.html' % wikifile.split('.wiki')[0]
    print('Saving %s in %s' % (wikifile, htmlfile))
    savehtmlfile(path, htmlfile, html)
    entry = search(wiki, path, wikifile)
    return entry


def searchengine(index):
    if os.path.exists('buscador.wiki'):
        index.sort()
        print('Creating search engine index with %s entries and %s keywords' % (len(index), sum([len(x[2]) for x in index])))
        indexplain = u"\n    index = new Array();"
        c = 0
        for entry in index:
            if entry[0].lower() in ['buscador', 'portada']:
                continue
            indexplain += u"\n    index[%s] = new Array('%s', '%s', '%s', new Array(%s));" % (c, entry[0], entry[1], entry[2], ', '.join(["'%s'" % x for x in entry[3]]))
            c += 1
        indexplain += u"\n    "
        
        f = open('buscador.wiki', 'r')
        wikicode = unicode(f.read(), 'utf-8')
        f.close()
        f = open('buscador.wiki', 'w')
        wikicode = u'%s//index start%s//index end%s' % (wikicode.split(u'//index start')[0], indexplain, wikicode.split(u'//index end')[1])
        f.write(wikicode.encode('utf-8'))
        f.close()
        processfile('.', 'buscador.wiki')


def readwikidirs():
    wikidirs = []
    if os.path.exists('wikidirs'):
        f = open('wikidirs', 'r')
        wikidirs = unicode(f.read(), 'utf-8').strip().splitlines()
        f.close()
    return wikidirs


def main():
    if len(sys.argv) < 2:
        print('Error, parameter needed. Supported params are --all or filename to process')
        sys.exit()
    
    wikifiles = []
    if sys.argv[1] == '--all':
        dirs = readwikidirs()
        for path in dirs:
            listdir = os.listdir(path)
            for filename in listdir:
                if filename.endswith('.wiki'):
                    wikifiles.append([path, filename])
        sitemap(wikifiles)
    else:
        path = '.'
        filename = sys.argv[1]
        if '/' in filename:
            path = '/'.join(filename.split('/')[:-1])
        wikifiles = [[path, filename]]
    
    index = []
    for path, filename in wikifiles:
        entry = processfile(path, filename)
        if entry:
            index.append(entry)
    
    if sys.argv[1] == '--all':
        searchengine(index)


if __name__ == '__main__':
    main()
