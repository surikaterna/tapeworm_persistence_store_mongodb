/** @type {import('jest').Config} */
module.exports = {
  roots: ['<rootDir>/lib', '<rootDir>/test'],
  transform: {
    '^.+\\.js$': ['es-jest']
  },

  testRegex: '(/test/.*|(\\.|/)(test|spec))\\.js$',
  moduleDirectories: ['node_modules', 'lib'],
  moduleFileExtensions: ['js', 'json', 'node']
};
