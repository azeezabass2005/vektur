use crate::errors::LexerError;

#[derive(Debug, Clone)]
pub enum TokenType {
    Select,
    From,
    Where,

    String(String),
    Number(String),

    Identifier(String),
    Asterisk,

    Equal,
    NotEqual,
    GreaterThan,
    LessThan,

    Comma,
    Semicolon,

    // End of Input
    EOF
}

pub struct Token {
    kind: TokenType,
    line: usize,
    column: usize,
}

impl Token {
    fn new(kind: TokenType) -> Self {
        Self {
            kind,
            line: 1,
            column: 1
        }
    }
}

pub fn tokenize(sql: &str) -> Result<Vec<Token>, LexerError> {
    let mut tokens: Vec<Token> = Vec::new();
    let mut current_token = String::new();
    let mut token_type: Option<TokenType> = None;
    // I need to restructure this iterator cos:
    // I need to be able to peek for some multi character operators
    // I also need to keep track of the line and column for better error feedback
    for char in sql.chars() {
        match char {
            ' ' | '\n' | '\t' => {
                if !current_token.is_empty() {
                    if let Some(tok_type) = &token_type {
                        tokens.push(Token::new(tok_type.clone()));
                    };
                    current_token.clear();
                    token_type = None;
                }
            },
            '=' | '<' | '>' | '!' | '*' => {
                if !current_token.is_empty() {
                    if let Some(tok_type) = &token_type {
                        tokens.push(Token::new(tok_type.clone()));
                        current_token.clear();
                    };
                    if char == '!' || char == '<' || char == '>' {
                        // TODO: I will continue from here tomorrow
                    }

                }
            },
            _ => {

            }
        }
    };
    Ok(tokens)
}