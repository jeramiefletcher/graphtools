def printVersion(testversion, pyfilie, version):
    pyfilie = pyfilie.capitalize()
    PrintVersion = 'Tested with ' + pyfilie + ' Version: {}'
    print('\n'
          + '-----------------------------------'
          + '-----------------------------------'
          + '\n' + 'TaseCase Version: ' + testversion + ' \n'
          + PrintVersion.format(version) + ' \n'
          + '-----------------------------------'
          + '-----------------------------------' + '\n '
          )
